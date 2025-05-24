# ONNX Runtime Web

ONNX Runtime Web is a Javascript library for running ONNX models on browsers and on Node.js.

ONNX Runtime Web has adopted WebAssembly and WebGL technologies for providing an optimized ONNX model inference runtime for both CPUs and GPUs.

### Why ONNX models

The [Open Neural Network Exchange](http://onnx.ai/) (ONNX) is an open standard for representing machine learning models. The biggest advantage of ONNX is that it allows interoperability across different open source AI frameworks, which itself offers more flexibility for AI frameworks adoption.

### Why ONNX Runtime Web

With ONNX Runtime Web, web developers can score models directly on browsers with various benefits including reducing server-client communication and protecting user privacy, as well as offering install-free and cross-platform in-browser ML experience.

ONNX Runtime Web can run on both CPU and GPU. On CPU side, [WebAssembly](https://developer.mozilla.org/en-US/docs/WebAssembly) is adopted to execute the model at near-native speed. ONNX Runtime Web compiles the native ONNX Runtime CPU engine into WebAssembly backend by using Emscripten, so it supports most functionalities native ONNX Runtime offers, including full ONNX operator coverage, multi-threading, [ONNX Runtime Quantization](https://www.onnxruntime.ai/docs/how-to/quantization.html) as well as [ONNX Runtime Mobile](https://onnxruntime.ai/docs/tutorials/mobile/). For performance acceleration with GPUs, ONNX Runtime Web leverages WebGL, a popular standard for accessing GPU capabilities. We are keeping improving op coverage and optimizing performance in WebGL backend.

See [Compatibility](#Compatibility) and [Operators Supported](#Operators) for a list of platforms and operators ONNX Runtime Web currently supports.

## Usage

- See [Get started](https://onnxruntime.ai/docs/get-started/with-javascript/web.html) as a landing page for ONNX Runtime Web documentation.

- Refer to [ONNX Runtime JavaScript examples](https://github.com/microsoft/onnxruntime-inference-examples/tree/main/js) for samples and tutorials.

- See also [ONNX Runtime Web API reference](https://onnxruntime.ai/docs/api/js/index.html) for detailed API documentation.

## Documents

### Development

Refer to the following links for development information:

- [Development](../README.md#Development)
- [Build](../README.md#Build-2)
- [Test](../README.md#Test)
- [Debugging](../README.md#Debugging)
- [Generating Document](../README.md#Generating-Document)

### Compatibility

| EPs/Browsers      | Chrome/Edge (Windows) | Chrome/Edge (Android) | Chrome/Edge (MacOS) | Chrome/Edge (iOS) | Safari (MacOS)    | Safari (iOS)      | Firefox (Windows) | Node.js           |
| ----------------- | --------------------- | --------------------- | ------------------- | ----------------- | ----------------- | ----------------- | ----------------- | ----------------- |
| WebAssembly (CPU) | ✔️                    | ✔️                    | ✔️                  | ✔️                | ✔️                | ✔️                | ✔️                | ✔️<sup>\[1]</sup> |
| WebGPU            | ✔️<sup>\[2]</sup>     | ✔️<sup>\[3]</sup>     | ✔️                  | ❌                | ❌                | ❌                | ❌                | ❌                |
| WebGL             | ✔️<sup>\[4]</sup>     | ✔️<sup>\[4]</sup>     | ✔️<sup>\[4]</sup>   | ✔️<sup>\[4]</sup> | ✔️<sup>\[4]</sup> | ✔️<sup>\[4]</sup> | ✔️<sup>\[4]</sup> | ❌                |
| WebNN             | ✔️<sup>\[5]</sup>     | ❌                    | ❌                  | ❌                | ❌                | ❌                | ❌                | ❌                |

- \[1]: Node.js only support single-threaded `wasm` EP.
- \[2]: WebGPU requires Chromium v113 or later on Windows. Float16 support requires Chrome v121 or later, and Edge v122 or later.
- \[3]: WebGPU requires Chromium v121 or later on Windows.
- \[4]: WebGL support is in maintenance mode. It is recommended to use WebGPU for better performance.
- \[5]: Requires to launch browser with commandline flag `--enable-features=WebMachineLearningNeuralNetwork`.

### Operators

#### WebAssembly backend

ONNX Runtime Web currently support all operators in [ai.onnx](https://github.com/onnx/onnx/blob/main/docs/Operators.md) and [ai.onnx.ml](https://github.com/onnx/onnx/blob/main/docs/Operators-ml.md).

#### WebGL backend

ONNX Runtime Web currently supports a subset of operators in [ai.onnx](https://github.com/onnx/onnx/blob/main/docs/Operators.md) operator set. See [webgl-operators.md](./docs/webgl-operators.md) for a complete, detailed list of which ONNX operators are supported by WebGL backend.

#### WebGPU backend

WebGPU backend is still an experimental feature. See [webgpu-operators.md](./docs/webgpu-operators.md) for a detailed list of which ONNX operators are supported by WebGPU backend.

#### WebNN backend

WebNN backend is still an experimental feature. See [webnn-operators.md](./docs/webnn-operators.md) for a detailed list of which ONNX operators are supported by WebNN backend.

## License

License information can be found [here](https://github.com/microsoft/onnxruntime/blob/main/README.md#license).
