#include "minilzo209/minilzo.h"

#include <node.h>
#include <v8.h>
#include <sstream>

#include <node_buffer.h>

#define GET_FUNCTION(A, B, C) FunctionTemplate::New(A, B)->GetFunction()
#define MAKE_STRING(A, B) String::NewFromUtf8(A, B)

#if NODE_MAJOR_VERSION >= 12
#define NODE_12

#undef GET_FUNCTION
#define GET_FUNCTION(A, B, C) FunctionTemplate::New(A, B)->GetFunction(C).ToLocalChecked()

#undef MAKE_STRING
#define MAKE_STRING(A, B) String::NewFromUtf8(A, B, NewStringType::kNormal).ToLocalChecked()
#endif

#define NEW_API (NODE_MAJOR_VERSION >= 10)

using namespace v8;

int compress(const unsigned char *input, unsigned char *output, lzo_uint in_len, lzo_uint& out_len) {
    char* wrkmem = (char*) malloc(LZO1X_1_MEM_COMPRESS);

    int result = lzo1x_1_compress(input, in_len, output, &out_len, wrkmem);

    free(wrkmem);

    return result;
}

lzo_uint decompress(const unsigned char *input, unsigned char *output, lzo_uint in_len, lzo_uint& out_len) {
    int r = lzo1x_decompress_safe(input, in_len, output, &out_len, NULL);

    if (r == LZO_E_OK)
        return out_len;
    else
        return r;
}

void js_compress(const v8::FunctionCallbackInfo<Value>& args) {
    Isolate* isolate = args.GetIsolate();
    HandleScope scope(isolate);

#if NEW_API
    Local<Context> context = isolate->GetCurrentContext();
    Local<Object> inputBuffer = args[0]->ToObject(context).ToLocalChecked();
    Local<Object> outputBuffer = args[1]->ToObject(context).ToLocalChecked();
#else
    Handle<Object> inputBuffer = args[0]->ToObject();
    Handle<Object> outputBuffer = args[1]->ToObject();
#endif

    lzo_uint input_len = node::Buffer::Length(inputBuffer);
    lzo_uint output_len = node::Buffer::Length(outputBuffer);

    int result = compress(  (unsigned char *) node::Buffer::Data(inputBuffer),
                            (unsigned char *) node::Buffer::Data(outputBuffer),
                            input_len,
                            output_len );

    Local<Object> ret = Object::New(isolate);

#ifdef NODE_12
    (void) ret->Set(context, MAKE_STRING(isolate, "err"), Number::New(isolate, result));
    (void) ret->Set(context, MAKE_STRING(isolate, "len"), Number::New(isolate, (int) output_len));
#else
    ret->Set(MAKE_STRING(isolate, "err"), Number::New(isolate, result));
    ret->Set(MAKE_STRING(isolate, "len"), Number::New(isolate, (int) output_len));
#endif

    args.GetReturnValue().Set(ret);
}

void js_decompress(const v8::FunctionCallbackInfo<Value>& args) {
    Isolate* isolate = args.GetIsolate();
    HandleScope scope(isolate);

#if NEW_API
    Local<Context> context = isolate->GetCurrentContext();
    Local<Object> inputBuffer = args[0]->ToObject(context).ToLocalChecked();
    Local<Object> outputBuffer = args[1]->ToObject(context).ToLocalChecked();
#else
    Handle<Object> inputBuffer = args[0]->ToObject();
    Handle<Object> outputBuffer = args[1]->ToObject();
#endif

    lzo_uint input_len = node::Buffer::Length(inputBuffer);
    lzo_uint output_len = node::Buffer::Length(outputBuffer);

    lzo_uint len = decompress(  (unsigned char *) node::Buffer::Data(inputBuffer),
                                (unsigned char *) node::Buffer::Data(outputBuffer),
                                input_len,
                                output_len);

    int err = (int) len < 0 ? (int) len : 0;

    Local<Object> ret = Object::New(isolate);

#ifdef NODE_12
    (void) ret->Set(context, MAKE_STRING(isolate, "err"), Number::New(isolate, err));
    (void) ret->Set(context, MAKE_STRING(isolate, "len"), Number::New(isolate, (int) len) );
#else
    ret->Set(MAKE_STRING(isolate, "err"), Number::New(isolate, err));
    ret->Set(MAKE_STRING(isolate, "len"), Number::New(isolate, (int) len) );
#endif

    args.GetReturnValue().Set(ret);
}

void Init(Local<Object> exports, Local<Context> context) {
    Isolate* isolate = context->GetIsolate();

    int init_result = lzo_init();

    if(init_result != LZO_E_OK) {
        std::stringstream ss;

        ss << "lzo_init() failed and returned `" << init_result << "`. ";
        ss << "Please report this on GitHub: https://github.com/schroffl/node-lzo/issues";

        Local<String> err = MAKE_STRING(isolate, ss.str().c_str());

        isolate->ThrowException(Exception::Error(err));

        return;
    }

    // Compression
    (void) exports->Set(
        context,
        MAKE_STRING(isolate, "compress"),
        GET_FUNCTION(isolate, js_compress, context)
    );

    // Decompression
    (void) exports->Set(
        context,
        MAKE_STRING(isolate, "decompress"),
        GET_FUNCTION(isolate, js_decompress, context)
    );

    // Current lzo version
    const char *version = lzo_version_string();
    (void) exports->Set(
        context,
        MAKE_STRING(isolate, "version"),
        MAKE_STRING(isolate, version)
    );

    // Date for current lzo version
    const char *date = lzo_version_date();
    (void) exports->Set(
        context,
        MAKE_STRING(isolate, "versionDate"),
        MAKE_STRING(isolate, date)
    );
}

#if (NODE_MAJOR_VERSION >= 10 && NODE_MINOR_VERSION >= 7) || NODE_MAJOR_VERSION >= 11
  // Initialize this addon to be context-aware. See Issue #11
  NODE_MODULE_INIT(/* exports, module, context */) {
      Init(exports, context);
  }
#else
  // For backwards compatibility. See Issue #13
  NODE_MODULE(node_lzo, Init)
#endif
