# ONNX Runtime Node.js Binding

ONNX Runtime Node.js binding enables Node.js applications to run ONNX model inference.

## Usage

Install the latest stable version:

```
npm install onnxruntime-node
```

Refer to [ONNX Runtime JavaScript examples](https://github.com/microsoft/onnxruntime-inference-examples/tree/main/js) for samples and tutorials.

## Requirements

ONNXRuntime works on Node.js v16.x+ (recommend v20.x+) or Electron v15.x+ (recommend v28.x+).

The following table lists the supported versions of ONNX Runtime Node.js binding provided with pre-built binaries.

| EPs/Platforms | Windows x64 | Windows arm64 | Linux x64         | Linux arm64 | MacOS x64 | MacOS arm64 |
| ------------- | ----------- | ------------- | ----------------- | ----------- | --------- | ----------- |
| CPU           | ✔️          | ✔️            | ✔️                | ✔️          | ✔️        | ✔️          |
| DirectML      | ✔️          | ✔️            | ❌                | ❌          | ❌        | ❌          |
| CUDA          | ❌          | ❌            | ✔️<sup>\[1]</sup> | ❌          | ❌        | ❌          |

- \[1]: CUDA v11.8.

To use on platforms without pre-built binaries, you can build Node.js binding from source and consume it by `npm install <onnxruntime_repo_root>/js/node/`. See also [instructions](https://onnxruntime.ai/docs/build/inferencing.html#apis-and-language-bindings) for building ONNX Runtime Node.js binding locally.

# GPU Support

Right now, the Windows version supports only the DML provider. Linux x64 can use CUDA and TensorRT.

## CUDA EP Installation

To use CUDA EP, you need to install the CUDA EP binaries. By default, the CUDA EP binaries are installed automatically when you install the package. If you want to skip the installation, you can pass the `--onnxruntime-node-install-cuda=skip` flag to the installation command.

```
npm install onnxruntime-node --onnxruntime-node-install-cuda=skip
```

You can also use this flag to specify the version of the CUDA: (v11 or v12)

```
npm install onnxruntime-node --onnxruntime-node-install-cuda=v12
```

## License

License information can be found [here](https://github.com/microsoft/onnxruntime/blob/main/README.md#license).
