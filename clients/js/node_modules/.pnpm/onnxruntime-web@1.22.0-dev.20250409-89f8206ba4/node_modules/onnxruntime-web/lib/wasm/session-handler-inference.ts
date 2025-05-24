// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import {
  InferenceSession,
  InferenceSessionHandler,
  SessionHandler,
  Tensor,
  TRACE_FUNC_BEGIN,
  TRACE_FUNC_END,
} from 'onnxruntime-common';

import { SerializableInternalBuffer, TensorMetadata } from './proxy-messages';
import { copyFromExternalBuffer, createSession, endProfiling, releaseSession, run } from './proxy-wrapper';
import { isGpuBufferSupportedType, isMLTensorSupportedType } from './wasm-common';
import { isNode } from './wasm-utils-env';
import { loadFile } from './wasm-utils-load-file';

export const encodeTensorMetadata = (tensor: Tensor, getName: () => string): TensorMetadata => {
  switch (tensor.location) {
    case 'cpu':
      return [tensor.type, tensor.dims, tensor.data, 'cpu'];
    case 'gpu-buffer':
      return [tensor.type, tensor.dims, { gpuBuffer: tensor.gpuBuffer }, 'gpu-buffer'];
    case 'ml-tensor':
      return [tensor.type, tensor.dims, { mlTensor: tensor.mlTensor }, 'ml-tensor'];
    default:
      throw new Error(`invalid data location: ${tensor.location} for ${getName()}`);
  }
};

export const decodeTensorMetadata = (tensor: TensorMetadata): Tensor => {
  switch (tensor[3]) {
    case 'cpu':
      return new Tensor(tensor[0], tensor[2], tensor[1]);
    case 'gpu-buffer': {
      const dataType = tensor[0];
      if (!isGpuBufferSupportedType(dataType)) {
        throw new Error(`not supported data type: ${dataType} for deserializing GPU tensor`);
      }
      const { gpuBuffer, download, dispose } = tensor[2];
      return Tensor.fromGpuBuffer(gpuBuffer, { dataType, dims: tensor[1], download, dispose });
    }
    case 'ml-tensor': {
      const dataType = tensor[0];
      if (!isMLTensorSupportedType(dataType)) {
        throw new Error(`not supported data type: ${dataType} for deserializing MLTensor tensor`);
      }
      const { mlTensor, download, dispose } = tensor[2];
      return Tensor.fromMLTensor(mlTensor, { dataType, dims: tensor[1], download, dispose });
    }
    default:
      throw new Error(`invalid data location: ${tensor[3]}`);
  }
};

export class OnnxruntimeWebAssemblySessionHandler implements InferenceSessionHandler {
  private sessionId: number;

  inputNames: readonly string[];
  outputNames: readonly string[];
  inputMetadata: readonly InferenceSession.ValueMetadata[];
  outputMetadata: readonly InferenceSession.ValueMetadata[];

  async fetchModelAndCopyToWasmMemory(path: string): Promise<SerializableInternalBuffer> {
    // fetch model from url and move to wasm heap.
    return copyFromExternalBuffer(await loadFile(path));
  }

  async loadModel(pathOrBuffer: string | Uint8Array, options?: InferenceSession.SessionOptions): Promise<void> {
    TRACE_FUNC_BEGIN();
    let model: Parameters<typeof createSession>[0];

    if (typeof pathOrBuffer === 'string') {
      if (isNode) {
        // node
        model = await loadFile(pathOrBuffer);
      } else {
        // browser
        // fetch model and copy to wasm heap.
        model = await this.fetchModelAndCopyToWasmMemory(pathOrBuffer);
      }
    } else {
      model = pathOrBuffer;
    }

    [this.sessionId, this.inputNames, this.outputNames, this.inputMetadata, this.outputMetadata] = await createSession(
      model,
      options,
    );
    TRACE_FUNC_END();
  }

  async dispose(): Promise<void> {
    return releaseSession(this.sessionId);
  }

  async run(
    feeds: SessionHandler.FeedsType,
    fetches: SessionHandler.FetchesType,
    options: InferenceSession.RunOptions,
  ): Promise<SessionHandler.ReturnType> {
    TRACE_FUNC_BEGIN();
    const inputArray: Tensor[] = [];
    const inputIndices: number[] = [];
    Object.entries(feeds).forEach((kvp) => {
      const name = kvp[0];
      const tensor = kvp[1];
      const index = this.inputNames.indexOf(name);
      if (index === -1) {
        throw new Error(`invalid input '${name}'`);
      }
      inputArray.push(tensor);
      inputIndices.push(index);
    });

    const outputArray: Array<Tensor | null> = [];
    const outputIndices: number[] = [];
    Object.entries(fetches).forEach((kvp) => {
      const name = kvp[0];
      const tensor = kvp[1];
      const index = this.outputNames.indexOf(name);
      if (index === -1) {
        throw new Error(`invalid output '${name}'`);
      }
      outputArray.push(tensor);
      outputIndices.push(index);
    });

    const inputs = inputArray.map((t, i) =>
      encodeTensorMetadata(t, () => `input "${this.inputNames[inputIndices[i]]}"`),
    );
    const outputs = outputArray.map((t, i) =>
      t ? encodeTensorMetadata(t, () => `output "${this.outputNames[outputIndices[i]]}"`) : null,
    );

    const results = await run(this.sessionId, inputIndices, inputs, outputIndices, outputs, options);

    const resultMap: SessionHandler.ReturnType = {};
    for (let i = 0; i < results.length; i++) {
      resultMap[this.outputNames[outputIndices[i]]] = outputArray[i] ?? decodeTensorMetadata(results[i]);
    }
    TRACE_FUNC_END();
    return resultMap;
  }

  startProfiling(): void {
    // TODO: implement profiling
  }

  endProfiling(): void {
    void endProfiling(this.sessionId);
  }
}
