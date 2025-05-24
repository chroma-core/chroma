// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Backend, InferenceSession, InferenceSessionHandler, SessionHandler } from 'onnxruntime-common';

import { Binding, binding, initOrt } from './binding';

class OnnxruntimeSessionHandler implements InferenceSessionHandler {
  #inferenceSession: Binding.InferenceSession;

  constructor(pathOrBuffer: string | Uint8Array, options: InferenceSession.SessionOptions) {
    initOrt();

    this.#inferenceSession = new binding.InferenceSession();
    if (typeof pathOrBuffer === 'string') {
      this.#inferenceSession.loadModel(pathOrBuffer, options);
    } else {
      this.#inferenceSession.loadModel(pathOrBuffer.buffer, pathOrBuffer.byteOffset, pathOrBuffer.byteLength, options);
    }
    this.inputNames = this.#inferenceSession.inputNames;
    this.outputNames = this.#inferenceSession.outputNames;
  }

  async dispose(): Promise<void> {
    this.#inferenceSession.dispose();
  }

  readonly inputNames: string[];
  readonly outputNames: string[];

  startProfiling(): void {
    // startProfiling is a no-op.
    //
    // if sessionOptions.enableProfiling is true, profiling will be enabled when the model is loaded.
  }
  endProfiling(): void {
    this.#inferenceSession.endProfiling();
  }

  async run(
    feeds: SessionHandler.FeedsType,
    fetches: SessionHandler.FetchesType,
    options: InferenceSession.RunOptions,
  ): Promise<SessionHandler.ReturnType> {
    return new Promise((resolve, reject) => {
      setImmediate(() => {
        try {
          resolve(this.#inferenceSession.run(feeds, fetches, options));
        } catch (e) {
          // reject if any error is thrown
          reject(e);
        }
      });
    });
  }
}

class OnnxruntimeBackend implements Backend {
  async init(): Promise<void> {
    return Promise.resolve();
  }

  async createInferenceSessionHandler(
    pathOrBuffer: string | Uint8Array,
    options?: InferenceSession.SessionOptions,
  ): Promise<InferenceSessionHandler> {
    return new Promise((resolve, reject) => {
      setImmediate(() => {
        try {
          resolve(new OnnxruntimeSessionHandler(pathOrBuffer, options || {}));
        } catch (e) {
          // reject if any error is thrown
          reject(e);
        }
      });
    });
  }
}

export const onnxruntimeBackend = new OnnxruntimeBackend();
export const listSupportedBackends = binding.listSupportedBackends;
