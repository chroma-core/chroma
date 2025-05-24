// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env, InferenceSession } from 'onnxruntime-common';

import {
  OrtWasmMessage,
  SerializableInternalBuffer,
  SerializableSessionMetadata,
  SerializableTensorMetadata,
  TensorMetadata,
} from './proxy-messages';
import * as core from './wasm-core-impl';
import { initializeWebAssembly } from './wasm-factory';
import {
  importProxyWorker,
  inferWasmPathPrefixFromScriptSrc,
  isEsmImportMetaUrlHardcodedAsFileUri,
} from './wasm-utils-import';

const isProxy = (): boolean => !!env.wasm.proxy && typeof document !== 'undefined';
let proxyWorker: Worker | undefined;
let initializing = false;
let initialized = false;
let aborted = false;
let temporaryObjectUrl: string | undefined;

type PromiseCallbacks<T = void> = [resolve: (result: T) => void, reject: (reason: unknown) => void];
let initWasmCallbacks: PromiseCallbacks;
const queuedCallbacks: Map<OrtWasmMessage['type'], Array<PromiseCallbacks<unknown>>> = new Map();

const enqueueCallbacks = (type: OrtWasmMessage['type'], callbacks: PromiseCallbacks<unknown>): void => {
  const queue = queuedCallbacks.get(type);
  if (queue) {
    queue.push(callbacks);
  } else {
    queuedCallbacks.set(type, [callbacks]);
  }
};

const ensureWorker = (): void => {
  if (initializing || !initialized || aborted || !proxyWorker) {
    throw new Error('worker not ready');
  }
};

const onProxyWorkerMessage = (ev: MessageEvent<OrtWasmMessage>): void => {
  switch (ev.data.type) {
    case 'init-wasm':
      initializing = false;
      if (ev.data.err) {
        aborted = true;
        initWasmCallbacks[1](ev.data.err);
      } else {
        initialized = true;
        initWasmCallbacks[0]();
      }
      if (temporaryObjectUrl) {
        URL.revokeObjectURL(temporaryObjectUrl);
        temporaryObjectUrl = undefined;
      }
      break;
    case 'init-ep':
    case 'copy-from':
    case 'create':
    case 'release':
    case 'run':
    case 'end-profiling': {
      const callbacks = queuedCallbacks.get(ev.data.type)!;
      if (ev.data.err) {
        callbacks.shift()![1](ev.data.err);
      } else {
        callbacks.shift()![0](ev.data.out!);
      }
      break;
    }
    default:
  }
};

export const initializeWebAssemblyAndOrtRuntime = async (): Promise<void> => {
  if (initialized) {
    return;
  }
  if (initializing) {
    throw new Error("multiple calls to 'initWasm()' detected.");
  }
  if (aborted) {
    throw new Error("previous call to 'initWasm()' failed.");
  }

  initializing = true;

  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    return new Promise<void>((resolve, reject) => {
      proxyWorker?.terminate();

      void importProxyWorker().then(([objectUrl, worker]) => {
        try {
          proxyWorker = worker;
          proxyWorker.onerror = (ev: ErrorEvent) => reject(ev);
          proxyWorker.onmessage = onProxyWorkerMessage;
          initWasmCallbacks = [resolve, reject];
          const message: OrtWasmMessage = { type: 'init-wasm', in: env };

          // if the proxy worker is loaded from a blob URL, we need to make sure the path information is not lost.
          //
          // when `env.wasm.wasmPaths` is not set, we need to pass the path information to the worker.
          //
          if (!BUILD_DEFS.ENABLE_BUNDLE_WASM_JS && !message.in!.wasm.wasmPaths && objectUrl) {
            // for a build not bundled the wasm JS, we need to pass the path prefix to the worker.
            // the path prefix will be used to resolve the path to both the wasm JS and the wasm file.
            const inferredWasmPathPrefix = inferWasmPathPrefixFromScriptSrc();
            if (inferredWasmPathPrefix) {
              message.in!.wasm.wasmPaths = inferredWasmPathPrefix;
            }
          }

          if (
            BUILD_DEFS.IS_ESM &&
            BUILD_DEFS.ENABLE_BUNDLE_WASM_JS &&
            !message.in!.wasm.wasmPaths &&
            (objectUrl || isEsmImportMetaUrlHardcodedAsFileUri)
          ) {
            // for a build bundled the wasm JS, if either of the following conditions is met:
            // - the proxy worker is loaded from a blob URL
            // - `import.meta.url` is a file URL, it means it is overwriten by the bundler.
            //
            // in either case, the path information is lost, we need to pass the path of the .wasm file to the worker.
            // we need to use the bundler preferred URL format:
            // new URL('filename', import.meta.url)
            // so that the bundler can handle the file using corresponding loaders.
            message.in!.wasm.wasmPaths = {
              wasm: !BUILD_DEFS.DISABLE_JSEP
                ? new URL('ort-wasm-simd-threaded.jsep.wasm', BUILD_DEFS.ESM_IMPORT_META_URL).href
                : new URL('ort-wasm-simd-threaded.wasm', BUILD_DEFS.ESM_IMPORT_META_URL).href,
            };
          }
          proxyWorker.postMessage(message);
          temporaryObjectUrl = objectUrl;
        } catch (e) {
          reject(e);
        }
      }, reject);
    });
  } else {
    try {
      await initializeWebAssembly(env.wasm);
      await core.initRuntime(env);
      initialized = true;
    } catch (e) {
      aborted = true;
      throw e;
    } finally {
      initializing = false;
    }
  }
};

export const initializeOrtEp = async (epName: string): Promise<void> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    ensureWorker();
    return new Promise<void>((resolve, reject) => {
      enqueueCallbacks('init-ep', [resolve, reject]);
      const message: OrtWasmMessage = { type: 'init-ep', in: { epName, env } };
      proxyWorker!.postMessage(message);
    });
  } else {
    await core.initEp(env, epName);
  }
};

export const copyFromExternalBuffer = async (buffer: Uint8Array): Promise<SerializableInternalBuffer> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    ensureWorker();
    return new Promise<SerializableInternalBuffer>((resolve, reject) => {
      enqueueCallbacks('copy-from', [resolve, reject]);
      const message: OrtWasmMessage = { type: 'copy-from', in: { buffer } };
      proxyWorker!.postMessage(message, [buffer.buffer]);
    });
  } else {
    return core.copyFromExternalBuffer(buffer);
  }
};

export const createSession = async (
  model: SerializableInternalBuffer | Uint8Array,
  options?: InferenceSession.SessionOptions,
): Promise<SerializableSessionMetadata> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    // check unsupported options
    if (options?.preferredOutputLocation) {
      throw new Error('session option "preferredOutputLocation" is not supported for proxy.');
    }
    ensureWorker();
    return new Promise<SerializableSessionMetadata>((resolve, reject) => {
      enqueueCallbacks('create', [resolve, reject]);
      const message: OrtWasmMessage = { type: 'create', in: { model, options: { ...options } } };
      const transferable: Transferable[] = [];
      if (model instanceof Uint8Array) {
        transferable.push(model.buffer);
      }
      proxyWorker!.postMessage(message, transferable);
    });
  } else {
    return core.createSession(model, options);
  }
};

export const releaseSession = async (sessionId: number): Promise<void> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    ensureWorker();
    return new Promise<void>((resolve, reject) => {
      enqueueCallbacks('release', [resolve, reject]);
      const message: OrtWasmMessage = { type: 'release', in: sessionId };
      proxyWorker!.postMessage(message);
    });
  } else {
    core.releaseSession(sessionId);
  }
};

export const run = async (
  sessionId: number,
  inputIndices: number[],
  inputs: TensorMetadata[],
  outputIndices: number[],
  outputs: Array<TensorMetadata | null>,
  options: InferenceSession.RunOptions,
): Promise<TensorMetadata[]> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    // check inputs location
    if (inputs.some((t) => t[3] !== 'cpu')) {
      throw new Error('input tensor on GPU is not supported for proxy.');
    }
    // check outputs location
    if (outputs.some((t) => t)) {
      throw new Error('pre-allocated output tensor is not supported for proxy.');
    }
    ensureWorker();
    return new Promise<SerializableTensorMetadata[]>((resolve, reject) => {
      enqueueCallbacks('run', [resolve, reject]);
      const serializableInputs = inputs as SerializableTensorMetadata[]; // every input is on CPU.
      const message: OrtWasmMessage = {
        type: 'run',
        in: { sessionId, inputIndices, inputs: serializableInputs, outputIndices, options },
      };
      proxyWorker!.postMessage(message, core.extractTransferableBuffers(serializableInputs));
    });
  } else {
    return core.run(sessionId, inputIndices, inputs, outputIndices, outputs, options);
  }
};

export const endProfiling = async (sessionId: number): Promise<void> => {
  if (!BUILD_DEFS.DISABLE_WASM_PROXY && isProxy()) {
    ensureWorker();
    return new Promise<void>((resolve, reject) => {
      enqueueCallbacks('end-profiling', [resolve, reject]);
      const message: OrtWasmMessage = { type: 'end-profiling', in: sessionId };
      proxyWorker!.postMessage(message);
    });
  } else {
    core.endProfiling(sessionId);
  }
};
