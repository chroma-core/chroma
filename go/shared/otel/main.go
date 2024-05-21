package otel

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pingcap/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelCode "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var tracer trace.Tracer

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
	// Init with a default tracer if not already set. (Unit test)
	if tracer == nil {
		tracer = otel.GetTracerProvider().Tracer("LOCAL")
	}
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
	span.SetAttributes(attribute.String("rpc.method", info.FullMethod))

	// Calls the handler
	h, err := handler(ctx, req)
	if err != nil {
		// Handle and log the error.
		handleError(span, info, err)
		return nil, err
	}

	// Set the status to OK upon success.
	span.SetStatus(otelCode.Ok, "ok")
	span.SetAttributes(attribute.String("rpc.status_code", "ok"))
	log.Info("RPC call", zap.String("method", info.FullMethod), zap.String("status", "ok"))

	return h, nil
}

// handleError logs and annotates the span with details of the encountered error.
func handleError(span trace.Span, info *grpc.UnaryServerInfo, err error) {
	st, _ := status.FromError(err)
	span.SetStatus(otelCode.Error, "error")
	span.SetAttributes(
		attribute.String("rpc.status_code", st.Code().String()),
		attribute.String("rpc.message", st.Message()),
		attribute.String("rpc.error", st.Err().Error()),
	)
	log.Error("RPC call", zap.String("method", info.FullMethod), zap.String("status", st.Code().String()), zap.String("error", st.Err().Error()), zap.String("message", st.Message()))

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
			otlptracegrpc.WithDialOption(),
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
