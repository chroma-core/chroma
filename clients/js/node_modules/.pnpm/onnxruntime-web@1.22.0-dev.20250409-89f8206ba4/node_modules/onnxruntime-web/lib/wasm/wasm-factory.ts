// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Env } from 'onnxruntime-common';

import type { OrtWasmModule } from './wasm-types';
import { importWasmModule, inferWasmPathPrefixFromScriptSrc } from './wasm-utils-import';

let wasm: OrtWasmModule | undefined;
let initialized = false;
let initializing = false;
let aborted = false;

const isMultiThreadSupported = (): boolean => {
  // If 'SharedArrayBuffer' is not available, WebAssembly threads will not work.
  if (typeof SharedArrayBuffer === 'undefined') {
    return false;
  }

  try {
    // Test for transferability of SABs (for browsers. needed for Firefox)
    // https://groups.google.com/forum/#!msg/mozilla.dev.platform/IHkBZlHETpA/dwsMNchWEQAJ
    if (typeof MessageChannel !== 'undefined') {
      new MessageChannel().port1.postMessage(new SharedArrayBuffer(1));
    }

    // Test for WebAssembly threads capability (for both browsers and Node.js)
    // This typed array is a WebAssembly program containing threaded instructions.
    return WebAssembly.validate(
      new Uint8Array([
        0, 97, 115, 109, 1, 0, 0, 0, 1, 4, 1, 96, 0, 0, 3, 2, 1, 0, 5, 4, 1, 3, 1, 1, 10, 11, 1, 9, 0, 65, 0, 254, 16,
        2, 0, 26, 11,
      ]),
    );
  } catch (e) {
    return false;
  }
};

const isSimdSupported = (): boolean => {
  try {
    // Test for WebAssembly SIMD capability (for both browsers and Node.js)
    // This typed array is a WebAssembly program containing SIMD instructions.

    // The binary data is generated from the following code by wat2wasm:
    //
    // (module
    //   (type $t0 (func))
    //   (func $f0 (type $t0)
    //     (drop
    //       (i32x4.dot_i16x8_s
    //         (i8x16.splat
    //           (i32.const 0))
    //         (v128.const i32x4 0x00000000 0x00000000 0x00000000 0x00000000)))))

    return WebAssembly.validate(
      new Uint8Array([
        0, 97, 115, 109, 1, 0, 0, 0, 1, 4, 1, 96, 0, 0, 3, 2, 1, 0, 10, 30, 1, 28, 0, 65, 0, 253, 15, 253, 12, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 253, 186, 1, 26, 11,
      ]),
    );
  } catch (e) {
    return false;
  }
};

const isRelaxedSimdSupported = (): boolean => {
  try {
    // Test for WebAssembly Relaxed SIMD capability (for both browsers and Node.js)
    // This typed array is a WebAssembly program containing Relaxed SIMD instructions.

    // The binary data is generated from the following code by wat2wasm:
    // (module
    //   (func (result v128)
    //      i32.const 1
    //      i8x16.splat
    //      i32.const 2
    //      i8x16.splat
    //      i32.const 3
    //      i8x16.splat
    //      i32x4.relaxed_dot_i8x16_i7x16_add_s
    //   )
    //  )
    return WebAssembly.validate(
      new Uint8Array([
        0, 97, 115, 109, 1, 0, 0, 0, 1, 5, 1, 96, 0, 1, 123, 3, 2, 1, 0, 10, 19, 1, 17, 0, 65, 1, 253, 15, 65, 2, 253,
        15, 65, 3, 253, 15, 253, 147, 2, 11,
      ]),
    );
  } catch (e) {
    return false;
  }
};

export const initializeWebAssembly = async (flags: Env.WebAssemblyFlags): Promise<void> => {
  if (initialized) {
    return Promise.resolve();
  }
  if (initializing) {
    throw new Error("multiple calls to 'initializeWebAssembly()' detected.");
  }
  if (aborted) {
    throw new Error("previous call to 'initializeWebAssembly()' failed.");
  }

  initializing = true;

  // wasm flags are already initialized
  const timeout = flags.initTimeout!;
  let numThreads = flags.numThreads!;

  // ensure SIMD is supported
  if (flags.simd === false) {
    // skip SIMD feature checking as it is disabled explicitly by user
  } else if (flags.simd === 'relaxed') {
    // check if relaxed SIMD is supported
    if (!isRelaxedSimdSupported()) {
      throw new Error('Relaxed WebAssembly SIMD is not supported in the current environment.');
    }
  } else if (!isSimdSupported()) {
    throw new Error('WebAssembly SIMD is not supported in the current environment.');
  }

  // check if multi-threading is supported
  const multiThreadSupported = isMultiThreadSupported();
  if (numThreads > 1 && !multiThreadSupported) {
    if (typeof self !== 'undefined' && !self.crossOriginIsolated) {
      // eslint-disable-next-line no-console
      console.warn(
        'env.wasm.numThreads is set to ' +
          numThreads +
          ', but this will not work unless you enable crossOriginIsolated mode. ' +
          'See https://web.dev/cross-origin-isolation-guide/ for more info.',
      );
    }

    // eslint-disable-next-line no-console
    console.warn(
      'WebAssembly multi-threading is not supported in the current environment. ' + 'Falling back to single-threading.',
    );

    // set flags.numThreads to 1 so that OrtInit() will not create a global thread pool.
    flags.numThreads = numThreads = 1;
  }

  const wasmPaths = flags.wasmPaths;
  const wasmPrefixOverride = typeof wasmPaths === 'string' ? wasmPaths : undefined;
  const mjsPathOverrideFlag = (wasmPaths as Env.WasmFilePaths)?.mjs;
  const mjsPathOverride = (mjsPathOverrideFlag as URL)?.href ?? mjsPathOverrideFlag;
  const wasmPathOverrideFlag = (wasmPaths as Env.WasmFilePaths)?.wasm;
  const wasmPathOverride = (wasmPathOverrideFlag as URL)?.href ?? wasmPathOverrideFlag;
  const wasmBinaryOverride = flags.wasmBinary;

  const [objectUrl, ortWasmFactory] = await importWasmModule(mjsPathOverride, wasmPrefixOverride, numThreads > 1);

  let isTimeout = false;

  const tasks: Array<Promise<void>> = [];

  // promise for timeout
  if (timeout > 0) {
    tasks.push(
      new Promise((resolve) => {
        setTimeout(() => {
          isTimeout = true;
          resolve();
        }, timeout);
      }),
    );
  }

  // promise for module initialization
  tasks.push(
    new Promise((resolve, reject) => {
      const config: Partial<OrtWasmModule> = {
        /**
         * The number of threads. WebAssembly will create (Module.numThreads - 1) workers. If it is 1, no worker will be
         * created.
         */
        numThreads,
      };

      if (wasmBinaryOverride) {
        // Set a custom buffer which contains the WebAssembly binary. This will skip the wasm file fetching.
        config.wasmBinary = wasmBinaryOverride;
      } else if (wasmPathOverride || wasmPrefixOverride) {
        // A callback function to locate the WebAssembly file. The function should return the full path of the file.
        //
        // Since Emscripten 3.1.58, this function is only called for the .wasm file.
        config.locateFile = (fileName) => wasmPathOverride ?? wasmPrefixOverride + fileName;
      } else if (mjsPathOverride && mjsPathOverride.indexOf('blob:') !== 0) {
        // if mjs path is specified, use it as the base path for the .wasm file.
        config.locateFile = (fileName) => new URL(fileName, mjsPathOverride).href;
      } else if (objectUrl) {
        const inferredWasmPathPrefix = inferWasmPathPrefixFromScriptSrc();
        if (inferredWasmPathPrefix) {
          // if the wasm module is preloaded, use the inferred wasm path as the base path for the .wasm file.
          config.locateFile = (fileName) => inferredWasmPathPrefix + fileName;
        }
      }

      ortWasmFactory(config).then(
        // wasm module initialized successfully
        (module) => {
          initializing = false;
          initialized = true;
          wasm = module;
          resolve();
          if (objectUrl) {
            URL.revokeObjectURL(objectUrl);
          }
        },
        // wasm module failed to initialize
        (what) => {
          initializing = false;
          aborted = true;
          reject(what);
        },
      );
    }),
  );

  await Promise.race(tasks);

  if (isTimeout) {
    throw new Error(`WebAssembly backend initializing failed due to timeout: ${timeout}ms`);
  }
};

export const getInstance = (): OrtWasmModule => {
  if (initialized && wasm) {
    return wasm;
  }

  throw new Error('WebAssembly is not initialized yet.');
};

export const dispose = (): void => {
  if (initialized && !initializing && !aborted) {
    // TODO: currently "PThread.terminateAllThreads()" is not exposed in the wasm module.
    //       And this function is not yet called by any code.
    //       If it is needed in the future, we should expose it in the wasm module and uncomment the following line.

    // wasm?.PThread?.terminateAllThreads();
    wasm = undefined;

    initializing = false;
    initialized = false;
    aborted = true;
  }
};
