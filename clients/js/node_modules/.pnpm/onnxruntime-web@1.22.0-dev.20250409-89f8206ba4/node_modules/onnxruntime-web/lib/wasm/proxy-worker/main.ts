// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/// <reference lib="webworker" />

//
// * type hack for "HTMLImageElement"
//
// in typescript, the type of "HTMLImageElement" is defined in lib.dom.d.ts, which is conflict with lib.webworker.d.ts.
// when we use webworker, the lib.webworker.d.ts will be used, which does not have HTMLImageElement defined.
//
// we will get the following errors complaining that HTMLImageElement is not defined:
//
// ====================================================================================================================
//
// ../common/dist/cjs/tensor-factory.d.ts:187:29 - error TS2552: Cannot find name 'HTMLImageElement'. Did you mean
// 'HTMLLIElement'?
//
// 187     fromImage(imageElement: HTMLImageElement, options?: TensorFromImageElementOptions):
// Promise<TypedTensor<'float32'> | TypedTensor<'uint8'>>;
//                                 ~~~~~~~~~~~~~~~~
//
// node_modules/@webgpu/types/dist/index.d.ts:83:7 - error TS2552: Cannot find name 'HTMLImageElement'. Did you mean
// 'HTMLLIElement'?
//
// 83     | HTMLImageElement
//          ~~~~~~~~~~~~~~~~
//
// ====================================================================================================================
//
// `HTMLImageElement` is only used in type declaration and not in real code. So we define it as `unknown` here to
// bypass the type check.

//
// * type hack for "document"
//
// in typescript, the type of "document" is defined in lib.dom.d.ts, so it's not available in webworker.
//
// we will get the following errors complaining that document is not defined:
//
// ====================================================================================================================
//
// lib/wasm/wasm-utils-import.ts:7:33 - error TS2584: Cannot find name 'document'. Do you need to change your target
// library? Try changing the 'lib' compiler option to include 'dom'.
//
// 7 export const scriptSrc = typeof document !== 'undefined' ? (document?.currentScript as HTMLScriptElement)?.src :
//                                   ~~~~~~~~
//
// lib/wasm/wasm-utils-import.ts:7:61 - error TS2584: Cannot find name 'document'. Do you need to change your target
// library? Try changing the 'lib' compiler option to include 'dom'.
//
// 7 export const scriptSrc = typeof document !== 'undefined' ? (document?.currentScript as HTMLScriptElement)?.src :
//                                                               ~~~~~~~~
//
// lib/wasm/wasm-utils-import.ts:7:88 - error TS2552: Cannot find name 'HTMLScriptElement'. Did you mean
// 'HTMLLIElement'?
//
// 7 export const scriptSrc = typeof document !== 'undefined' ? (document?.currentScript as HTMLScriptElement)?.src :
//                                                                                          ~~~~~~~~~~~~~~~~~
// ====================================================================================================================
//
// `document` is used to get the current script URL, which is not available in webworker. This file is served as a
// "dual" file for entries of both webworker and the esm module.
//
declare global {
  type HTMLImageElement = unknown;
  type HTMLScriptElement = { src?: string };
  const document: undefined | { currentScript?: HTMLScriptElement };
}

/**
 * @summary
 *
 * This file is served as a "dual" file for both entries of the following:
 * - The proxy worker itself.
 *   - When used as a worker, it listens to the messages from the main thread and performs the corresponding operations.
 *   - Should be imported directly using `new Worker()` in the main thread.
 *
 * - The ESM module that creates the proxy worker (as a worker launcher).
 *   - When used as a worker launcher, it creates the proxy worker and returns it.
 *   - Should be imported using `import()` in the main thread, with the query parameter `import=1`.
 *
 * This file will be always compiling into ESM format.
 */

import type { OrtWasmMessage, SerializableTensorMetadata } from '../proxy-messages.js';
import {
  createSession,
  copyFromExternalBuffer,
  endProfiling,
  extractTransferableBuffers,
  initEp,
  initRuntime,
  releaseSession,
  run,
} from '../wasm-core-impl.js';
import { initializeWebAssembly } from '../wasm-factory.js';
import { scriptSrc } from '../wasm-utils-import.js';

const WORKER_NAME = 'ort-wasm-proxy-worker';
const isProxyWorker = globalThis.self?.name === WORKER_NAME;

if (isProxyWorker) {
  // Worker thread
  self.onmessage = (ev: MessageEvent<OrtWasmMessage>): void => {
    const { type, in: message } = ev.data;
    try {
      switch (type) {
        case 'init-wasm':
          initializeWebAssembly(message!.wasm).then(
            () => {
              initRuntime(message!).then(
                () => {
                  postMessage({ type });
                },
                (err) => {
                  postMessage({ type, err });
                },
              );
            },
            (err) => {
              postMessage({ type, err });
            },
          );
          break;
        case 'init-ep': {
          const { epName, env } = message!;
          initEp(env, epName).then(
            () => {
              postMessage({ type });
            },
            (err) => {
              postMessage({ type, err });
            },
          );
          break;
        }
        case 'copy-from': {
          const { buffer } = message!;
          const bufferData = copyFromExternalBuffer(buffer);
          postMessage({ type, out: bufferData } as OrtWasmMessage);
          break;
        }
        case 'create': {
          const { model, options } = message!;
          createSession(model, options).then(
            (sessionMetadata) => {
              postMessage({ type, out: sessionMetadata } as OrtWasmMessage);
            },
            (err) => {
              postMessage({ type, err });
            },
          );
          break;
        }
        case 'release':
          releaseSession(message!);
          postMessage({ type });
          break;
        case 'run': {
          const { sessionId, inputIndices, inputs, outputIndices, options } = message!;
          run(sessionId, inputIndices, inputs, outputIndices, new Array(outputIndices.length).fill(null), options).then(
            (outputs) => {
              if (outputs.some((o) => o[3] !== 'cpu')) {
                postMessage({ type, err: 'Proxy does not support non-cpu tensor location.' });
              } else {
                postMessage(
                  { type, out: outputs } as OrtWasmMessage,
                  extractTransferableBuffers([...inputs, ...outputs] as SerializableTensorMetadata[]),
                );
              }
            },
            (err) => {
              postMessage({ type, err });
            },
          );
          break;
        }
        case 'end-profiling':
          endProfiling(message!);
          postMessage({ type });
          break;
        default:
      }
    } catch (err) {
      postMessage({ type, err } as OrtWasmMessage);
    }
  };
}

export default isProxyWorker
  ? null
  : (urlOverride?: string) =>
      new Worker(urlOverride ?? scriptSrc!, { type: BUILD_DEFS.IS_ESM ? 'module' : 'classic', name: WORKER_NAME });
