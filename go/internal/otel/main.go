package otel

import (
	"context"
	"encoding/hex"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelCode "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var tracer trace.Tracer

// encodeTraceID encodes a TraceID to a hexadecimal string.
func encodeTraceID(t trace.TraceID) string {
	return hex.EncodeToString(t[:])
}

// encodeSpanID encodes a SpanID to a hexadecimal string.
func encodeSpanID(s trace.SpanID) string {
	return hex.EncodeToString(s[:])
}

func decodeTraceID(encodedSpanID string) (t trace.TraceID, err error) {
	var spanBytes []byte
	spanBytes, err = hex.DecodeString(encodedSpanID)
	if err != nil {
		err = fmt.Errorf("failed to decode spanID: %w", err)
		return
	}
	copy(t[:], spanBytes)
	return
}

func decodeSpanID(encodedSpanID string) (s trace.SpanID, err error) {
	var spanBytes []byte
	spanBytes, err = hex.DecodeString(encodedSpanID)
	if err != nil {
		err = fmt.Errorf("failed to decode spanID: %w", err)
		return
	}
	copy(s[:], spanBytes)
	return
}

// ServerGrpcInterceptor is a gRPC server interceptor for tracing and optional metadata-based context enhancement.
func ServerGrpcInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	tracer := otel.GetTracerProvider().Tracer("")

	// Attempt to retrieve metadata, but proceed normally if not present.
	md, _ := metadata.FromIncomingContext(ctx)

	// Attempt to decode and apply trace and span IDs if present, without failing on their absence.
	spanIdValue := decodeMetadataValue(md, "chroma-spanid")
	traceIdValue := decodeMetadataValue(md, "chroma-traceid")

	var spanContext trace.SpanContext
	if spanIdValue != "" && traceIdValue != "" {
		if spanId, err := decodeSpanID(spanIdValue); err == nil {
			if traceId, err := decodeTraceID(traceIdValue); err == nil {
				spanContext = trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: traceId,
					SpanID:  spanId,
				})
				// Only set the remote span context if both trace and span IDs are valid and decoded.
				ctx = trace.ContextWithRemoteSpanContext(ctx, spanContext)
			}
		}
	}
	var span trace.Span
	ctx, span = tracer.Start(ctx, "Request "+info.FullMethod)
	defer span.End()

	// Calls the handler
	h, err := handler(ctx, req)
	if err != nil {
		// Handle and log the error.
		handleError(span, err)
		return nil, err
	}

	// Set the status to OK upon success.
	span.SetStatus(otelCode.Ok, "ok")
	span.SetAttributes(attribute.String("rpc.status_code", "ok"))
	span.SetAttributes(attribute.String("rpc.method", info.FullMethod))

	return h, nil
}

// How to use it:
// conn, err := grpc.Dial("your_grpc_server_address", grpc.WithUnaryInterceptor(unaryClientInterceptor()))
func ClientGrpcInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var span trace.Span
		ctx, span = tracer.Start(ctx, "RPC "+method)
		span.SetAttributes(attribute.String("rpc.method", method))

		defer span.End()
		traceID := span.SpanContext().TraceID()
		spanID := span.SpanContext().SpanID()
		md := metadata.New(map[string]string{
			"chroma-traceid": encodeTraceID(traceID),
			"chroma-spanid":  encodeSpanID(spanID),
		})
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Proceed with the invocation with the new context.
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Extract status code from the error.
		st, ok := status.FromError(err)
		span.SetAttributes(attribute.String("rpc.status_code", st.Code().String()))
		span.SetAttributes(attribute.String("rpc.message", st.Message()))
		if !ok {
			span.SetStatus(otelCode.Error, st.Code().String())
		} else {
			span.SetStatus(otelCode.Ok, "ok")
		}
		return err
	}
}

// handleError logs and annotates the span with details of the encountered error.
func handleError(span trace.Span, err error) {
	st, _ := status.FromError(err)
	span.SetStatus(otelCode.Error, "error")
	span.SetAttributes(
		attribute.String("rpc.status_code", st.Code().String()),
		attribute.String("rpc.message", st.Message()),
		attribute.String("rpc.error", st.Err().Error()),
	)
}

// decodeMetadataValue safely extracts a value from metadata, allowing for missing keys.
func decodeMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

type TracingConfig struct {
	Endpoint string
	Service  string
}

func InitTracing(ctx context.Context, config *TracingConfig) (err error) {
	var exp *otlptrace.Exporter
	exp, err = otlptrace.New(
		ctx,
		otlptracegrpc.NewClient(
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(config.Endpoint),
			otlptracegrpc.WithDialOption(grpc.WithBlock()), // Useful for waiting until the connection is up.
		),
	)
	if err != nil {
		return
	}

	// Create a new tracer provider with a batch span processor and the OTLP exporter.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(config.Service),
		)),
	)

	otel.SetTracerProvider(tp)
	tracer = otel.Tracer(config.Service)
	return
}
