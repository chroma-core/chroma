// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env as envImpl } from './env-impl.js';
import { TryGetGlobalType } from './type-helper.js';

export declare namespace Env {
  export type WasmPathPrefix = string;
  export interface WasmFilePaths {
    /**
     * Specify the override path for the main .wasm file.
     *
     * This path should be an absolute path.
     *
     * If not modified, the filename of the .wasm file is:
     * - `ort-wasm-simd-threaded.wasm` for default build
     * - `ort-wasm-simd-threaded.jsep.wasm` for JSEP build (with WebGPU and WebNN)
     */
    wasm?: URL | string;
    /**
     * Specify the override path for the main .mjs file.
     *
     * This path should be an absolute path.
     *
     * If not modified, the filename of the .mjs file is:
     * - `ort-wasm-simd-threaded.mjs` for default build
     * - `ort-wasm-simd-threaded.jsep.mjs` for JSEP build (with WebGPU and WebNN)
     */
    mjs?: URL | string;
  }
  export type WasmPrefixOrFilePaths = WasmPathPrefix | WasmFilePaths;
  export interface WebAssemblyFlags {
    /**
     * set or get number of thread(s). If omitted or set to 0, number of thread(s) will be determined by system. If set
     * to 1, no worker thread will be spawned.
     *
     * This setting is available only when WebAssembly multithread feature is available in current context.
     *
     * @defaultValue `0`
     */
    numThreads?: number;

    /**
     * set or get a boolean value indicating whether to enable SIMD. If set to false, SIMD will be forcely disabled.
     *
     * This setting is available only when WebAssembly SIMD feature is available in current context.
     *
     * @defaultValue `true`
     *
     * @deprecated This property is deprecated. Since SIMD is supported by all major JavaScript engines, non-SIMD
     * build is no longer provided. This property will be removed in future release.
     */
    simd?: boolean;

    /**
     * set or get a boolean value indicating whether to enable trace.
     *
     * @defaultValue `false`
     *
     * @deprecated Use `env.trace` instead. If `env.trace` is set, this property will be ignored.
     */
    trace?: boolean;

    /**
     * Set or get a number specifying the timeout for initialization of WebAssembly backend, in milliseconds. A zero
     * value indicates no timeout is set.
     *
     * @defaultValue `0`
     */
    initTimeout?: number;

    /**
     * Set a custom URL prefix to the .wasm/.mjs files, or an object of overrides for both .wasm/.mjs file. The override
     * path should be an absolute path.
     */
    wasmPaths?: WasmPrefixOrFilePaths;

    /**
     * Set a custom buffer which contains the WebAssembly binary. If this property is set, the `wasmPaths` property will
     * be ignored.
     */
    wasmBinary?: ArrayBufferLike | Uint8Array;

    /**
     * Set or get a boolean value indicating whether to proxy the execution of main thread to a worker thread.
     *
     * @defaultValue `false`
     */
    proxy?: boolean;
  }

  export interface WebGLFlags {
    /**
     * Set or get the WebGL Context ID (webgl or webgl2).
     *
     * @defaultValue `'webgl2'`
     */
    contextId?: 'webgl' | 'webgl2';
    /**
     * Get the WebGL rendering context.
     */
    readonly context: WebGLRenderingContext;
    /**
     * Set or get the maximum batch size for matmul. 0 means to disable batching.
     *
     * @deprecated
     */
    matmulMaxBatchSize?: number;
    /**
     * Set or get the texture cache mode.
     *
     * @defaultValue `'full'`
     */
    textureCacheMode?: 'initializerOnly' | 'full';
    /**
     * Set or get the packed texture mode
     *
     * @defaultValue `false`
     */
    pack?: boolean;
    /**
     * Set or get whether enable async download.
     *
     * @defaultValue `false`
     */
    async?: boolean;
  }

  export interface WebGpuProfilingDataV1TensorMetadata {
    dims: readonly number[];
    dataType: string;
  }
  export interface WebGpuProfilingDataV1 {
    version: 1;
    inputsMetadata: readonly WebGpuProfilingDataV1TensorMetadata[];
    outputsMetadata: readonly WebGpuProfilingDataV1TensorMetadata[];
    kernelId: number;
    kernelType: string;
    kernelName: string;
    programName: string;
    startTime: number;
    endTime: number;
  }

  export type WebGpuProfilingData = WebGpuProfilingDataV1;

  export interface WebGpuFlags {
    /**
     * Set or get the profiling mode.
     *
     * @deprecated Use `env.webgpu.profiling.mode` instead. If `env.webgpu.profiling.mode` is set, this property will be
     * ignored.
     */
    profilingMode?: 'off' | 'default';
    /**
     * Set or get the profiling configuration.
     */
    profiling: {
      /**
       * Set or get the profiling mode.
       *
       * @defaultValue `'off'`
       */
      mode?: 'off' | 'default';

      /**
       * Set or get a callback function when a profiling data is received. If not set, the profiling data will be
       * printed to console.
       */
      ondata?: (data: WebGpuProfilingData) => void;
    };
    /**
     * Set or get the power preference.
     *
     * Setting this property only has effect before the first WebGPU inference session is created. The value will be
     * used as options for `navigator.gpu.requestAdapter()`.
     *
     * See {@link https://gpuweb.github.io/gpuweb/#dictdef-gpurequestadapteroptions} for more details.
     *
     * @defaultValue `undefined`
     *
     * @deprecated Create your own GPUAdapter, use it to create a GPUDevice instance and set {@link device} property if
     * you want to use a specific power preference.
     */
    powerPreference?: 'low-power' | 'high-performance';
    /**
     * Set or get the force fallback adapter flag.
     *
     * Setting this property only has effect before the first WebGPU inference session is created. The value will be
     * used as options for `navigator.gpu.requestAdapter()`.
     *
     * See {@link https://gpuweb.github.io/gpuweb/#dictdef-gpurequestadapteroptions} for more details.
     *
     * @defaultValue `undefined`
     *
     * @deprecated Create your own GPUAdapter, use it to create a GPUDevice instance and set {@link device} property if
     * you want to use a specific fallback option.
     */
    forceFallbackAdapter?: boolean;
    /**
     * Set or get the adapter for WebGPU.
     *
     * Setting this property only has effect before the first WebGPU inference session is created. The value will be
     * used as the GPU adapter for the underlying WebGPU backend to create GPU device.
     *
     * If this property is not set, it will be available to get after the first WebGPU inference session is created. The
     * value will be the GPU adapter that created by the underlying WebGPU backend.
     *
     * When use with TypeScript, the type of this property is `GPUAdapter` defined in "@webgpu/types".
     *
     * @deprecated It is no longer recommended to use this property. The latest WebGPU spec adds `GPUDevice.adapterInfo`
     * (https://www.w3.org/TR/webgpu/#dom-gpudevice-adapterinfo), which allows to get the adapter information from the
     * device. When it's available, there is no need to set/get the {@link adapter} property.
     */
    adapter: TryGetGlobalType<'GPUAdapter'>;
    /**
     * Set or get the GPU device for WebGPU.
     *
     * There are 3 valid scenarios of accessing this property:
     * - Set a value before the first WebGPU inference session is created. The value will be used by the WebGPU backend
     * to perform calculations. If the value is not a `GPUDevice` object, an error will be thrown.
     * - Get the value before the first WebGPU inference session is created. This will try to create a new GPUDevice
     * instance. Returns a `Promise` that resolves to a `GPUDevice` object.
     * - Get the value after the first WebGPU inference session is created. Returns a resolved `Promise` to the
     * `GPUDevice` object used by the WebGPU backend.
     */
    get device(): Promise<TryGetGlobalType<'GPUDevice'>>;
    set device(value: TryGetGlobalType<'GPUDevice'>);
    /**
     * Set or get whether validate input content.
     *
     * @defaultValue `false`
     */
    validateInputContent?: boolean;
  }
}

export interface Env {
  /**
   * set the severity level for logging.
   *
   * @defaultValue `'warning'`
   */
  logLevel?: 'verbose' | 'info' | 'warning' | 'error' | 'fatal';

  /**
   * Indicate whether run in debug mode.
   *
   * @defaultValue `false`
   */
  debug?: boolean;

  /**
   * set or get a boolean value indicating whether to enable trace.
   *
   * @defaultValue `false`
   */
  trace?: boolean;

  /**
   * Get version of the current package.
   */
  readonly versions: {
    readonly common: string;
    readonly web?: string;
    readonly node?: string;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    readonly 'react-native'?: string;
  };

  /**
   * Represent a set of flags for WebAssembly
   */
  readonly wasm: Env.WebAssemblyFlags;

  /**
   * Represent a set of flags for WebGL
   */
  readonly webgl: Env.WebGLFlags;

  /**
   * Represent a set of flags for WebGPU
   */
  readonly webgpu: Env.WebGpuFlags;

  [name: string]: unknown;
}

/**
 * Represent a set of flags as a global singleton.
 */
export const env: Env = envImpl;
