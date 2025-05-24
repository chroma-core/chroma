// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { InferenceSession, OnnxValue, Tensor, TensorConstructor, env } from 'onnxruntime-common';

type SessionOptions = InferenceSession.SessionOptions;
type FeedsType = {
  [name: string]: OnnxValue;
};
type FetchesType = {
  [name: string]: OnnxValue | null;
};
type ReturnType = {
  [name: string]: OnnxValue;
};
type RunOptions = InferenceSession.RunOptions;

/**
 * Binding exports a simple synchronized inference session object wrap.
 */
export declare namespace Binding {
  export interface InferenceSession {
    loadModel(modelPath: string, options: SessionOptions): void;
    loadModel(buffer: ArrayBuffer, byteOffset: number, byteLength: number, options: SessionOptions): void;

    readonly inputNames: string[];
    readonly outputNames: string[];

    run(feeds: FeedsType, fetches: FetchesType, options: RunOptions): ReturnType;

    endProfiling(): void;

    dispose(): void;
  }

  export interface InferenceSessionConstructor {
    new (): InferenceSession;
  }

  export interface SupportedBackend {
    name: string;
    bundled: boolean;
  }
}

// export native binding
export const binding =
  // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
  require(`../bin/napi-v3/${process.platform}/${process.arch}/onnxruntime_binding.node`) as {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    InferenceSession: Binding.InferenceSessionConstructor;
    listSupportedBackends: () => Binding.SupportedBackend[];
    initOrtOnce: (logLevel: number, tensorConstructor: TensorConstructor) => void;
  };

let ortInitialized = false;
export const initOrt = (): void => {
  if (!ortInitialized) {
    ortInitialized = true;
    let logLevel = 2;
    if (env.logLevel) {
      switch (env.logLevel) {
        case 'verbose':
          logLevel = 0;
          break;
        case 'info':
          logLevel = 1;
          break;
        case 'warning':
          logLevel = 2;
          break;
        case 'error':
          logLevel = 3;
          break;
        case 'fatal':
          logLevel = 4;
          break;
        default:
          throw new Error(`Unsupported log level: ${env.logLevel}`);
      }
    }
    binding.initOrtOnce(logLevel, Tensor);
  }
};
