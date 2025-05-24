// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import type { Env } from 'onnxruntime-common';

import { calculateTensorSizeInBytes, DataType } from '../wasm-common';

import type { OrtWasmModule } from '../wasm-types';

import type { WebGpuBackend } from './backend-webgpu';
import { LOG_DEBUG } from './log';
import type { TensorView } from './tensor-view';
import { ShapeUtil } from './util';
import type { AdapterInfo, ComputeContext, ComputeContextInputsOutputsMapping, ProgramInfo } from './webgpu/types';
import { WebNNBackend } from './backend-webnn';

/* eslint-disable no-bitwise */

class TensorViewImpl implements TensorView {
  constructor(
    private module: OrtWasmModule,
    public readonly dataType: number,
    public readonly data: number,
    public readonly dims: readonly number[],
  ) {}

  getFloat32Array(): Float32Array {
    if (this.dataType !== DataType.float) {
      throw new Error('Invalid data type');
    }
    const elementCount = ShapeUtil.size(this.dims);
    return elementCount === 0
      ? new Float32Array()
      : new Float32Array(this.module.HEAP8.buffer, this.data, elementCount);
  }

  getBigInt64Array(): BigInt64Array {
    if (this.dataType !== DataType.int64) {
      throw new Error('Invalid data type');
    }
    const elementCount = ShapeUtil.size(this.dims);
    return elementCount === 0
      ? new BigInt64Array()
      : new BigInt64Array(this.module.HEAP8.buffer, this.data, elementCount);
  }

  getInt32Array(): Int32Array {
    if (this.dataType !== DataType.int32) {
      throw new Error('Invalid data type');
    }
    const elementCount = ShapeUtil.size(this.dims);
    return elementCount === 0 ? new Int32Array() : new Int32Array(this.module.HEAP8.buffer, this.data, elementCount);
  }

  getUint16Array(): Uint16Array {
    if (this.dataType !== DataType.float16 && this.dataType !== DataType.uint16) {
      throw new Error('Invalid data type');
    }
    const elementCount = ShapeUtil.size(this.dims);
    return elementCount === 0 ? new Uint16Array() : new Uint16Array(this.module.HEAP8.buffer, this.data, elementCount);
  }

  reshape(newDims: readonly number[]): TensorView {
    if (ShapeUtil.size(newDims) !== ShapeUtil.size(this.dims)) {
      throw new Error('Invalid new shape');
    }
    return new TensorViewImpl(this.module, this.dataType, this.data, newDims);
  }
}

class ComputeContextImpl implements ComputeContext {
  readonly adapterInfo: AdapterInfo;
  readonly opKernelContext: number;
  readonly inputs: readonly TensorView[];
  readonly outputCount: number;
  get kernelCustomData(): { [key: string]: unknown } {
    return this.backend.currentKernelCustomData;
  }
  get customDataBuffer(): Uint8Array {
    return this.module.HEAPU8.subarray(this.customDataOffset, this.customDataOffset + this.customDataSize);
  }
  private customDataOffset = 0;
  private customDataSize = 0;
  constructor(
    private module: OrtWasmModule,
    private backend: WebGpuBackend,
    contextDataOffset: number,
  ) {
    this.adapterInfo = backend.adapterInfo;

    // extract context data
    const ptrSize = module.PTR_SIZE;
    let dataIndex = contextDataOffset / module.PTR_SIZE;
    const type = ptrSize === 4 ? 'i32' : 'i64';
    this.opKernelContext = Number(module.getValue(ptrSize * dataIndex++, type));
    const inputCount = Number(module.getValue(ptrSize * dataIndex++, type));
    this.outputCount = Number(module.getValue(ptrSize * dataIndex++, type));
    this.customDataOffset = Number(module.getValue(ptrSize * dataIndex++, '*'));
    this.customDataSize = Number(module.getValue(ptrSize * dataIndex++, type));

    const inputs: TensorView[] = [];
    for (let i = 0; i < inputCount; i++) {
      const dataType = Number(module.getValue(ptrSize * dataIndex++, type));
      const data = Number(module.getValue(ptrSize * dataIndex++, '*'));
      const dim = Number(module.getValue(ptrSize * dataIndex++, type));
      const dims: number[] = [];
      for (let d = 0; d < dim; d++) {
        dims.push(Number(module.getValue(ptrSize * dataIndex++, type)));
      }
      inputs.push(new TensorViewImpl(module, dataType, data, dims));
    }
    this.inputs = inputs;
  }

  compute(program: ProgramInfo, inputsOutputsMapping?: ComputeContextInputsOutputsMapping): TensorView[] {
    // prepare inputs. inputs should always be valid data.
    const mappedInputs =
      inputsOutputsMapping?.inputs?.map((i) => (typeof i === 'number' ? this.inputs[i] : i)) ?? this.inputs;
    // prepare outputs.
    const outputIndices = inputsOutputsMapping?.outputs ?? [];
    const createKernelOutput = (index: number, dataType: number, dims: readonly number[]): TensorView =>
      new TensorViewImpl(this.module, dataType, this.output(index, dims), dims);
    const createTemporaryOutput = (dataType: number, dims: readonly number[]): TensorView => {
      const bufferSize = calculateTensorSizeInBytes(dataType, dims);
      if (!bufferSize) {
        throw new Error(`Unsupported data type: ${dataType}`);
      }
      const gpuDataId = bufferSize > 0 ? this.backend.gpuDataManager.create(bufferSize).id : 0;
      return new TensorViewImpl(this.module, dataType, gpuDataId, dims);
    };
    return this.backend.run(
      program,
      mappedInputs,
      outputIndices,
      createKernelOutput,
      createTemporaryOutput,
      this.outputCount,
    );
  }

  output(index: number, dims: readonly number[]): number {
    const stack = this.module.stackSave();
    try {
      const ptrSize = this.module.PTR_SIZE;
      const type = ptrSize === 4 ? 'i32' : 'i64';
      const data = this.module.stackAlloc((1 + dims.length) * ptrSize /* sizeof(size_t) */);
      this.module.setValue(data, dims.length, type);
      for (let i = 0; i < dims.length; i++) {
        this.module.setValue(data + ptrSize * (i + 1), dims[i], type);
      }
      return this.module._JsepOutput!(this.opKernelContext, index, data);
    } catch (e) {
      throw new Error(
        `Failed to generate kernel's output[${index}] with dims [${dims}]. ` +
          'If you are running with pre-allocated output, please make sure the output type/dims are correct. ' +
          `Error: ${e}`,
      );
    } finally {
      this.module.stackRestore(stack);
    }
  }
}

/**
 * Initialize JSEP with WebGPU backend.
 *
 * This function will be called after the WebAssembly module is loaded and initialized ("_OrtInit" is called), once for
 * each of the following EPs if they are specified:
 * - "webgpu"
 * - "webnn"
 *
 * For WebGPU, this function expects:
 *  - WebGPU is enabled in build (BUILD_DEFS.DISABLE_JSEP === false).
 *  - WebGPU is available in current environment. (a valid GPUAdapter is passed in)
 *
 * For WebNN, this function expects:
 * - WebNN is enabled in build (BUILD_DEFS.DISABLE_JSEP === false).
 * - WebNN is available in current environment. (navigator.ml is not undefined)
 *
 * If the WebAssembly module is not built with JSEP support, this function will throw an error. This will invalidate
 * 'webgpu'/'webnn' backend.
 *
 * @param name - the name of the EP, either "webgpu" or "webnn"
 * @param module - the ORT WebAssembly module
 * @param env - the ORT environment variable (ort.env)
 * @param gpuAdapter - the pre-created GPU adapter
 */
export const init = async (
  name: 'webgpu' | 'webnn',
  module: OrtWasmModule,
  env: Env,
  gpuAdapter?: GPUAdapter,
): Promise<void> => {
  const jsepInit = module.jsepInit;
  if (!jsepInit) {
    throw new Error('Failed to initialize JSEP. The WebAssembly module is not built with JSEP support.');
  }

  if (name === 'webgpu') {
    if (!BUILD_DEFS.USE_WEBGPU_EP) {
      // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
      const webGpuBackendImpl = require('./backend-webgpu').WebGpuBackend;
      const backend = new webGpuBackendImpl();
      await backend.initialize(env, gpuAdapter!);

      jsepInit('webgpu', [
        // backend
        backend,

        // jsepAlloc()
        (size: number) => backend.alloc(Number(size)),

        // jsepFree()
        (ptr: number) => backend.free(ptr),

        // jsepCopy(src, dst, size, isSourceGpu)
        (src: number, dst: number, size: number, isSourceGpu = false) => {
          if (isSourceGpu) {
            LOG_DEBUG(
              'verbose',
              () => `[WebGPU] jsepCopyGpuToGpu: src=${Number(src)}, dst=${Number(dst)}, size=${Number(size)}`,
            );
            backend.memcpy(Number(src), Number(dst));
          } else {
            LOG_DEBUG(
              'verbose',
              () =>
                `[WebGPU] jsepCopyCpuToGpu: dataOffset=${Number(src)}, gpuDataId=${Number(dst)}, size=${Number(size)}`,
            );
            const data = module.HEAPU8.subarray(Number(src >>> 0), Number(src >>> 0) + Number(size));
            backend.upload(Number(dst), data);
          }
        },

        // jsepCopyAsync(src, dst, size)
        async (gpuDataId: number, dataOffset: number, size: number): Promise<void> => {
          LOG_DEBUG(
            'verbose',
            () => `[WebGPU] jsepCopyGpuToCpu: gpuDataId=${gpuDataId}, dataOffset=${dataOffset}, size=${size}`,
          );

          await backend.download(Number(gpuDataId), () =>
            module.HEAPU8.subarray(Number(dataOffset) >>> 0, Number(dataOffset + size) >>> 0),
          );
        },

        // jsepCreateKernel
        (kernelType: string, kernelId: number, attribute: unknown) =>
          backend.createKernel(
            kernelType,
            Number(kernelId),
            attribute,
            module.UTF8ToString(module._JsepGetNodeName!(Number(kernelId))),
          ),

        // jsepReleaseKernel
        (kernel: number) => backend.releaseKernel(kernel),

        // jsepRun
        (kernel: number, contextDataOffset: number, sessionHandle: number, errors: Array<Promise<string | null>>) => {
          LOG_DEBUG(
            'verbose',
            () =>
              `[WebGPU] jsepRun: sessionHandle=${sessionHandle}, kernel=${kernel}, contextDataOffset=${contextDataOffset}`,
          );
          const context = new ComputeContextImpl(module, backend, Number(contextDataOffset));
          return backend.computeKernel(Number(kernel), context, errors);
        },
        // jsepCaptureBegin
        () => backend.captureBegin(),
        // jsepCaptureEnd
        () => backend.captureEnd(),
        // jsepReplay
        () => backend.replay(),
      ]);
    }
  } else {
    const backend = new WebNNBackend(env);
    jsepInit('webnn', [
      backend,
      // jsepReserveTensorId
      () => backend.reserveTensorId(),
      // jsepReleaseTensorId,
      (tensorId: number) => backend.releaseTensorId(tensorId),
      // jsepEnsureTensor
      async (sessionId: number | undefined, tensorId: number, onnxDataType: number, shape: number[], copyOld) =>
        backend.ensureTensor(sessionId, tensorId, onnxDataType, shape, copyOld),
      // jsepUploadTensor
      (tensorId: number, data: Uint8Array) => {
        backend.uploadTensor(tensorId, data);
      },
      // jsepDownloadTensor
      async (tensorId: number, dstBuffer: ArrayBufferView | ArrayBuffer) => backend.downloadTensor(tensorId, dstBuffer),
    ]);
  }
};
