// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// WebNN API currently does not have a TypeScript definition file. This file is a workaround with types generated from
// WebNN API specification.
// https://github.com/webmachinelearning/webnn/issues/677
/// <reference path="jsep/webnn/webnn.d.ts" />

import { Env, InferenceSession, Tensor } from 'onnxruntime-common';

import {
  SerializableInternalBuffer,
  SerializableSessionMetadata,
  SerializableTensorMetadata,
  TensorMetadata,
} from './proxy-messages';
import { setRunOptions } from './run-options';
import { setSessionOptions } from './session-options';
import {
  calculateTensorSizeInBytes,
  dataLocationStringToEnum,
  isGpuBufferSupportedType,
  isMLTensorSupportedType,
  logLevelStringToEnum,
  tensorDataTypeEnumToString,
  tensorDataTypeStringToEnum,
  tensorTypeToTypedArrayConstructor,
} from './wasm-common';
import { getInstance } from './wasm-factory';
import { allocWasmString, checkLastError } from './wasm-utils';
import { loadFile } from './wasm-utils-load-file';

// #region Initializations

/**
 * There are 4 different "initialization" steps for ORT. They happen in different places and different time.
 *
 * 1. JavaScript initialization for onnxruntime-common and onnxruntime-web.
 *    This is the first initialization step. In this step, onnxruntime-web calls onnxruntime-common's registerBackend()
 * function multiple times to register all the available backends. The backend registration is very fast. It only
 * registers the backend name with the uninitialized backend object. No heavy initialization is done in this step.
 *    Refer to web/lib/index.ts for the backend registration.
 *
 * 2. WebAssembly artifact initialization.
 *    This happens when any registered wasm backend is used for the first time (ie. `ort.InferenceSession.create()` is
 * called). In this step, onnxruntime-web does the followings:
 *     - create a proxy worker and make sure the proxy worker is ready to receive messages, if proxy is enabled.
 *     - perform feature detection, locate correct WebAssembly artifact path and call the Emscripten generated
 * JavaScript code to initialize the WebAssembly runtime.
 *         - if proxy is enabled, this step happens in the proxy worker using message 'init-wasm'.
 *         - downloading the 'ort-wasm{...}.wasm' file is done in this step.
 *         - if multi-thread is enabled, one or more webworker will be created to initialize the PThread threadpool.
 *
 * 3. ORT environment initialization.
 *    This happens after step 2. In this step, onnxruntime-web performs ONNX Runtime environment initialization.
 * Function `_OrtInit()` is called in this step.
 *     - if proxy is enabled, this step happens in the proxy worker using message 'init-ort'.
 *     - logging level (ort.env.logLevel) and thread number (ort.env.wasm.numThreads) are set in this step.
 *
 * 4. Session initialization.
 *    This happens when `ort.InferenceSession.create()` is called. Unlike the first 3 steps (they only called once),
 * this step will be done for each session. In this step, onnxruntime-web does the followings:
 *    If the parameter is a URL:
 *    - download the model data from the URL.
 *    - copy the model data to the WASM heap. (proxy: 'copy-from')
 *    - dereference the model buffer. This step allows the original ArrayBuffer to be garbage collected.
 *    - call `_OrtCreateSession()` to create the session. (proxy: 'create')
 *
 *    If the parameter is a Uint8Array object:
 *    - copy the model data to the WASM heap. (proxy: 'copy-from')
 *    - call `_OrtCreateSession()` to create the session. (proxy: 'create')
 *
 *
 */

/**
 * initialize ORT environment.
 *
 * @param numThreads SetGlobalIntraOpNumThreads(numThreads)
 * @param loggingLevel CreateEnv(static_cast<OrtLoggingLevel>(logging_level))
 */
const initOrt = (numThreads: number, loggingLevel: number): void => {
  const errorCode = getInstance()._OrtInit(numThreads, loggingLevel);
  if (errorCode !== 0) {
    checkLastError("Can't initialize onnxruntime.");
  }
};

/**
 * initialize runtime environment.
 * @param env passed in the environment config object.
 */
export const initRuntime = async (env: Env): Promise<void> => {
  // init ORT
  initOrt(env.wasm.numThreads!, logLevelStringToEnum(env.logLevel));
};

/**
 * perform EP specific initialization.
 *
 * @param env
 * @param epName
 */
export const initEp = async (env: Env, epName: string): Promise<void> => {
  // initialize ASYNCIFY support
  getInstance().asyncInit?.();

  if (epName === 'webgpu' && BUILD_DEFS.USE_WEBGPU_EP) {
    getInstance().webgpuInit!((device) => {
      env.webgpu.device = device;
    });
  }

  if (!BUILD_DEFS.DISABLE_JSEP) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
    const initJsep = require('./jsep/init').init;

    if (epName === 'webgpu' && !BUILD_DEFS.USE_WEBGPU_EP) {
      // perform WebGPU availability check
      if (typeof navigator === 'undefined' || !navigator.gpu) {
        throw new Error('WebGPU is not supported in current environment');
      }

      let adapter = env.webgpu.adapter as GPUAdapter | null;
      if (!adapter) {
        // if adapter is not set, request a new adapter.
        const powerPreference = env.webgpu.powerPreference;
        if (
          powerPreference !== undefined &&
          powerPreference !== 'low-power' &&
          powerPreference !== 'high-performance'
        ) {
          throw new Error(`Invalid powerPreference setting: "${powerPreference}"`);
        }
        const forceFallbackAdapter = env.webgpu.forceFallbackAdapter;
        if (forceFallbackAdapter !== undefined && typeof forceFallbackAdapter !== 'boolean') {
          throw new Error(`Invalid forceFallbackAdapter setting: "${forceFallbackAdapter}"`);
        }
        adapter = await navigator.gpu.requestAdapter({ powerPreference, forceFallbackAdapter });
        if (!adapter) {
          throw new Error(
            'Failed to get GPU adapter. ' +
              'You may need to enable flag "--enable-unsafe-webgpu" if you are using Chrome.',
          );
        }
      } else {
        // if adapter is set, validate it.
        if (
          typeof adapter.limits !== 'object' ||
          typeof adapter.features !== 'object' ||
          typeof adapter.requestDevice !== 'function'
        ) {
          throw new Error('Invalid GPU adapter set in `env.webgpu.adapter`. It must be a GPUAdapter object.');
        }
      }

      await initJsep('webgpu', getInstance(), env, adapter);
    }
    if (epName === 'webnn') {
      // perform WebNN availability check
      if (typeof navigator === 'undefined' || !(navigator as unknown as { ml: unknown }).ml) {
        throw new Error('WebNN is not supported in current environment');
      }

      await initJsep('webnn', getInstance(), env);
    }
  }
};

// #endregion Initializations

/**
 * valid data locations for input/output tensors.
 */
type SupportedTensorDataLocationForInputOutput = 'cpu' | 'cpu-pinned' | 'gpu-buffer' | 'ml-tensor';

type IOBindingState = {
  /**
   * the handle of IO binding.
   */
  readonly handle: number;

  /**
   * the preferred location for each output tensor.
   *
   * value is one of 'cpu', 'cpu-pinned', 'gpu-buffer', 'ml-tensor'.
   */
  readonly outputPreferredLocations: readonly SupportedTensorDataLocationForInputOutput[];

  /**
   * enum value of the preferred location for each output tensor.
   */
  readonly outputPreferredLocationsEncoded: readonly number[];
};

/**
 *  tuple elements are: InferenceSession ID; inputNamesUTF8Encoded; outputNamesUTF8Encoded; bindingState
 */
type SessionMetadata = [
  inferenceSessionId: number,
  inputNamesUTF8Encoded: number[],
  outputNamesUTF8Encoded: number[],
  bindingState: IOBindingState | null,
  enableGraphCapture: boolean,
  inputOutputBound: boolean,
];

const activeSessions = new Map<number, SessionMetadata>();

/**
 * get the input/output count of the session.
 * @param sessionHandle the handle representing the session. should be non-zero.
 * @returns a tuple including 2 numbers, representing the input count and output count.
 */
const getSessionInputOutputCount = (sessionHandle: number): [number, number] => {
  const wasm = getInstance();
  const stack = wasm.stackSave();
  try {
    const ptrSize = wasm.PTR_SIZE;
    const dataOffset = wasm.stackAlloc(2 * ptrSize);
    const errorCode = wasm._OrtGetInputOutputCount(sessionHandle, dataOffset, dataOffset + ptrSize);
    if (errorCode !== 0) {
      checkLastError("Can't get session input/output count.");
    }
    const type = ptrSize === 4 ? 'i32' : 'i64';
    return [Number(wasm.getValue(dataOffset, type)), Number(wasm.getValue(dataOffset + ptrSize, type))];
  } finally {
    wasm.stackRestore(stack);
  }
};

const getSessionInputOutputMetadata = (
  sessionHandle: number,
  index: number,
): [nameOffset: number, elementType: number, dims?: Array<number | string>] => {
  const wasm = getInstance();
  const stack = wasm.stackSave();
  let metadataOffset = 0;
  try {
    const ptrSize = wasm.PTR_SIZE;
    const dataOffset = wasm.stackAlloc(2 * ptrSize);
    const errorCode = wasm._OrtGetInputOutputMetadata(sessionHandle, index, dataOffset, dataOffset + ptrSize);
    if (errorCode !== 0) {
      checkLastError("Can't get session input/output metadata.");
    }
    const nameOffset = Number(wasm.getValue(dataOffset, '*'));
    metadataOffset = Number(wasm.getValue(dataOffset + ptrSize, '*'));
    // get element type
    const elementType = wasm.HEAP32[metadataOffset / 4];
    if (elementType === 0) {
      return [nameOffset, 0]; // non-tensor
    }

    // get dims count
    const dimsCount = wasm.HEAPU32[metadataOffset / 4 + 1];
    // get dims
    const dims: Array<number | string> = [];
    for (let i = 0; i < dimsCount; i++) {
      const symbolicDimNameOffset = Number(wasm.getValue(metadataOffset + 8 + i * ptrSize, '*'));
      dims.push(
        symbolicDimNameOffset !== 0
          ? wasm.UTF8ToString(symbolicDimNameOffset)
          : Number(wasm.getValue(metadataOffset + 8 + (i + dimsCount) * ptrSize, '*')),
      );
    }
    return [nameOffset, elementType, dims];
  } finally {
    wasm.stackRestore(stack);
    if (metadataOffset !== 0) {
      wasm._OrtFree(metadataOffset);
    }
  }
};

/**
 * allocate the memory and memcpy the external buffer.
 *
 * @param model - the external buffer containing the model data. Must not be the same buffer as the WASM heap.
 * @returns a 2-elements tuple - the pointer and size of the allocated buffer
 */
export const copyFromExternalBuffer = (model: Uint8Array): [number, number] => {
  const wasm = getInstance();
  const modelDataOffset = wasm._malloc(model.byteLength);
  if (modelDataOffset === 0) {
    throw new Error(`Can't create a session. failed to allocate a buffer of size ${model.byteLength}.`);
  }
  wasm.HEAPU8.set(model, modelDataOffset);
  return [modelDataOffset, model.byteLength];
};

/**
 * create an inference session from a model data buffer.
 *
 * @param modelData - either a Uint8Array object representing the model data, or a 2-elements tuple containing the
 *     pointer and size of the model data buffer.
 * @param options an optional session options object.
 * @returns a 3-elements tuple containing [session handle, input names, output names]
 */
export const createSession = async (
  modelData: Uint8Array | SerializableInternalBuffer,
  options?: InferenceSession.SessionOptions,
): Promise<SerializableSessionMetadata> => {
  let modelDataOffset: number, modelDataLength: number;
  const wasm = getInstance();

  if (Array.isArray(modelData)) {
    // if model data is an array, it must be a 2-elements tuple containing the pointer and size of the model data
    [modelDataOffset, modelDataLength] = modelData;
  } else if (modelData.buffer === wasm.HEAPU8.buffer) {
    // if model data uses the same buffer as the WASM heap, we don't need to copy it.
    [modelDataOffset, modelDataLength] = [modelData.byteOffset, modelData.byteLength];
  } else {
    // otherwise, copy the model data to the WASM heap.
    [modelDataOffset, modelDataLength] = copyFromExternalBuffer(modelData);
  }

  let sessionHandle = 0;
  let sessionOptionsHandle = 0;
  let ioBindingHandle = 0;
  let allocs: number[] = [];
  const inputNamesUTF8Encoded = [];
  const outputNamesUTF8Encoded = [];

  try {
    [sessionOptionsHandle, allocs] = await setSessionOptions(options);

    if (options?.externalData && wasm.mountExternalData) {
      const loadingPromises = [];
      for (const file of options.externalData) {
        const path = typeof file === 'string' ? file : file.path;
        loadingPromises.push(
          loadFile(typeof file === 'string' ? file : file.data).then((data) => {
            wasm.mountExternalData(path, data);
          }),
        );
      }

      // wait for all external data files to be loaded
      await Promise.all(loadingPromises);
    }

    for (const provider of options?.executionProviders ?? []) {
      const providerName = typeof provider === 'string' ? provider : provider.name;
      if (providerName === 'webnn') {
        wasm.shouldTransferToMLTensor = false;
        if (typeof provider !== 'string') {
          const webnnOptions = provider as InferenceSession.WebNNExecutionProviderOption;
          const context = (webnnOptions as InferenceSession.WebNNOptionsWithMLContext)?.context;
          const gpuDevice = (webnnOptions as InferenceSession.WebNNOptionsWebGpu)?.gpuDevice;
          const deviceType = (webnnOptions as InferenceSession.WebNNContextOptions)?.deviceType;
          const powerPreference = (webnnOptions as InferenceSession.WebNNContextOptions)?.powerPreference;
          if (context) {
            wasm.currentContext = context as MLContext;
          } else if (gpuDevice) {
            wasm.currentContext = await wasm.webnnCreateMLContext!(gpuDevice);
          } else {
            wasm.currentContext = await wasm.webnnCreateMLContext!({ deviceType, powerPreference });
          }
        } else {
          wasm.currentContext = await wasm.webnnCreateMLContext!();
        }
        break;
      }
    }

    sessionHandle = await wasm._OrtCreateSession(modelDataOffset, modelDataLength, sessionOptionsHandle);
    wasm.webgpuOnCreateSession?.(sessionHandle);
    if (sessionHandle === 0) {
      checkLastError("Can't create a session.");
    }

    wasm.jsepOnCreateSession?.();

    // clear current MLContext after session creation
    if (wasm.currentContext) {
      wasm.webnnRegisterMLContext!(sessionHandle, wasm.currentContext);
      wasm.currentContext = undefined;
      wasm.shouldTransferToMLTensor = true;
    }

    const [inputCount, outputCount] = getSessionInputOutputCount(sessionHandle);

    const enableGraphCapture = !!options?.enableGraphCapture;

    const inputNames = [];
    const outputNames = [];
    const inputMetadata: InferenceSession.ValueMetadata[] = [];
    const outputMetadata: InferenceSession.ValueMetadata[] = [];
    const outputPreferredLocations: SupportedTensorDataLocationForInputOutput[] = [];
    for (let i = 0; i < inputCount; i++) {
      const [nameOffset, elementType, shape] = getSessionInputOutputMetadata(sessionHandle, i);
      if (nameOffset === 0) {
        checkLastError("Can't get an input name.");
      }
      inputNamesUTF8Encoded.push(nameOffset);
      const name = wasm.UTF8ToString(nameOffset);
      inputNames.push(name);
      inputMetadata.push(
        elementType === 0
          ? { name, isTensor: false }
          : { name, isTensor: true, type: tensorDataTypeEnumToString(elementType), shape: shape! },
      );
    }
    for (let i = 0; i < outputCount; i++) {
      const [nameOffset, elementType, shape] = getSessionInputOutputMetadata(sessionHandle, i + inputCount);
      if (nameOffset === 0) {
        checkLastError("Can't get an output name.");
      }
      outputNamesUTF8Encoded.push(nameOffset);
      const nameString = wasm.UTF8ToString(nameOffset);
      outputNames.push(nameString);
      outputMetadata.push(
        elementType === 0
          ? { name: nameString, isTensor: false }
          : { name: nameString, isTensor: true, type: tensorDataTypeEnumToString(elementType), shape: shape! },
      );

      if (!BUILD_DEFS.DISABLE_JSEP) {
        if (enableGraphCapture && options?.preferredOutputLocation === undefined) {
          outputPreferredLocations.push('gpu-buffer');
          continue;
        }
        const location =
          typeof options?.preferredOutputLocation === 'string'
            ? options.preferredOutputLocation
            : (options?.preferredOutputLocation?.[nameString] ?? 'cpu');
        if (location !== 'cpu' && location !== 'cpu-pinned' && location !== 'gpu-buffer' && location !== 'ml-tensor') {
          throw new Error(`Not supported preferred output location: ${location}.`);
        }
        if (enableGraphCapture && location !== 'gpu-buffer') {
          throw new Error(
            `Not supported preferred output location: ${location}. Only 'gpu-buffer' location is supported when enableGraphCapture is true.`,
          );
        }
        outputPreferredLocations.push(location);
      }
    }

    // use IO binding only when at least one output is preferred to be on GPU.
    let bindingState: IOBindingState | null = null;
    if (!BUILD_DEFS.DISABLE_JSEP && outputPreferredLocations.some((l) => l === 'gpu-buffer' || l === 'ml-tensor')) {
      ioBindingHandle = wasm._OrtCreateBinding(sessionHandle);
      if (ioBindingHandle === 0) {
        checkLastError("Can't create IO binding.");
      }

      bindingState = {
        handle: ioBindingHandle,
        outputPreferredLocations,
        outputPreferredLocationsEncoded: outputPreferredLocations.map((l) => dataLocationStringToEnum(l)),
      };
    }

    activeSessions.set(sessionHandle, [
      sessionHandle,
      inputNamesUTF8Encoded,
      outputNamesUTF8Encoded,
      bindingState,
      enableGraphCapture,
      false,
    ]);
    return [sessionHandle, inputNames, outputNames, inputMetadata, outputMetadata];
  } catch (e) {
    inputNamesUTF8Encoded.forEach((buf) => wasm._OrtFree(buf));
    outputNamesUTF8Encoded.forEach((buf) => wasm._OrtFree(buf));

    if (ioBindingHandle !== 0) {
      if (wasm._OrtReleaseBinding(ioBindingHandle) !== 0) {
        checkLastError("Can't release IO binding.");
      }
    }

    if (sessionHandle !== 0) {
      if (wasm._OrtReleaseSession(sessionHandle) !== 0) {
        checkLastError("Can't release session.");
      }
    }
    throw e;
  } finally {
    wasm._free(modelDataOffset);
    if (sessionOptionsHandle !== 0) {
      if (wasm._OrtReleaseSessionOptions(sessionOptionsHandle) !== 0) {
        checkLastError("Can't release session options.");
      }
    }
    allocs.forEach((alloc) => wasm._free(alloc));

    // unmount external data if necessary
    wasm.unmountExternalData?.();
  }
};

export const releaseSession = (sessionId: number): void => {
  const wasm = getInstance();
  const session = activeSessions.get(sessionId);
  if (!session) {
    throw new Error(`cannot release session. invalid session id: ${sessionId}`);
  }
  const [sessionHandle, inputNamesUTF8Encoded, outputNamesUTF8Encoded, ioBindingState, enableGraphCapture] = session;

  if (ioBindingState) {
    if (enableGraphCapture) {
      if (wasm._OrtClearBoundOutputs(ioBindingState.handle) !== 0) {
        checkLastError("Can't clear bound outputs.");
      }
    }
    if (wasm._OrtReleaseBinding(ioBindingState.handle) !== 0) {
      checkLastError("Can't release IO binding.");
    }
  }

  wasm.jsepOnReleaseSession?.(sessionId);
  wasm.webnnOnReleaseSession?.(sessionId);
  wasm.webgpuOnReleaseSession?.(sessionId);

  inputNamesUTF8Encoded.forEach((buf) => wasm._OrtFree(buf));
  outputNamesUTF8Encoded.forEach((buf) => wasm._OrtFree(buf));
  if (wasm._OrtReleaseSession(sessionHandle) !== 0) {
    checkLastError("Can't release session.");
  }
  activeSessions.delete(sessionId);
};

export const prepareInputOutputTensor = async (
  tensor: TensorMetadata | null,
  tensorHandles: number[],
  allocs: number[],
  sessionId: number,
  tensorNameUTF8Encoded: number,
  index: number,
  enableGraphCapture = false,
): Promise<void> => {
  if (!tensor) {
    tensorHandles.push(0);
    return;
  }

  const wasm = getInstance();
  const ptrSize = wasm.PTR_SIZE;

  const dataType = tensor[0];
  const dims = tensor[1];
  const location = tensor[3];
  let actualLocation = location;

  let rawData: number;
  let dataByteLength: number;

  if (dataType === 'string' && (location === 'gpu-buffer' || location === 'ml-tensor')) {
    throw new Error('String tensor is not supported on GPU.');
  }

  if (enableGraphCapture && location !== 'gpu-buffer') {
    throw new Error(
      `External buffer must be provided for input/output index ${index} when enableGraphCapture is true.`,
    );
  }

  if (location === 'gpu-buffer') {
    const gpuBuffer = tensor[2].gpuBuffer;
    dataByteLength = calculateTensorSizeInBytes(tensorDataTypeStringToEnum(dataType), dims)!;

    if (BUILD_DEFS.USE_WEBGPU_EP) {
      const registerBuffer = wasm.webgpuRegisterBuffer;
      if (!registerBuffer) {
        throw new Error('Tensor location "gpu-buffer" is not supported without using WebGPU.');
      }

      rawData = registerBuffer(gpuBuffer, sessionId);
    } else {
      const registerBuffer = wasm.jsepRegisterBuffer;
      if (!registerBuffer) {
        throw new Error('Tensor location "gpu-buffer" is not supported without using WebGPU.');
      }
      rawData = registerBuffer(sessionId, index, gpuBuffer, dataByteLength);
    }
  } else if (location === 'ml-tensor') {
    const mlTensor = tensor[2].mlTensor as MLTensor;
    dataByteLength = calculateTensorSizeInBytes(tensorDataTypeStringToEnum(dataType), dims)!;

    const registerMLTensor = wasm.webnnRegisterMLTensor;
    if (!registerMLTensor) {
      throw new Error('Tensor location "ml-tensor" is not supported without using WebNN.');
    }
    rawData = registerMLTensor(sessionId, mlTensor, tensorDataTypeStringToEnum(dataType), dims);
  } else {
    const data = tensor[2];

    if (Array.isArray(data)) {
      // string tensor
      dataByteLength = ptrSize * data.length;
      rawData = wasm._malloc(dataByteLength);
      allocs.push(rawData);
      for (let i = 0; i < data.length; i++) {
        if (typeof data[i] !== 'string') {
          throw new TypeError(`tensor data at index ${i} is not a string`);
        }
        wasm.setValue(rawData + i * ptrSize, allocWasmString(data[i], allocs), '*');
      }
    } else {
      const isGraphInput = wasm.webnnIsGraphInput;
      if (dataType !== 'string' && isGraphInput) {
        const tensorName = wasm.UTF8ToString(tensorNameUTF8Encoded);
        // Promote the tensor to 'ml-tensor' if it is a graph input.
        if (isGraphInput(sessionId, tensorName)) {
          const dataTypeEnum = tensorDataTypeStringToEnum(dataType);
          dataByteLength = calculateTensorSizeInBytes(dataTypeEnum, dims)!;
          actualLocation = 'ml-tensor';
          const createTemporaryTensor = wasm.webnnCreateTemporaryTensor;
          const uploadTensor = wasm.webnnUploadTensor;
          if (!createTemporaryTensor || !uploadTensor) {
            throw new Error('Tensor location "ml-tensor" is not supported without using WebNN.');
          }
          const tensorId = await createTemporaryTensor(sessionId, dataTypeEnum, dims as number[]);
          uploadTensor(tensorId, new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
          rawData = tensorId;
        } else {
          dataByteLength = data.byteLength;
          rawData = wasm._malloc(dataByteLength);
          allocs.push(rawData);
          wasm.HEAPU8.set(new Uint8Array(data.buffer, data.byteOffset, dataByteLength), rawData);
        }
      } else {
        dataByteLength = data.byteLength;
        rawData = wasm._malloc(dataByteLength);
        allocs.push(rawData);
        wasm.HEAPU8.set(new Uint8Array(data.buffer, data.byteOffset, dataByteLength), rawData);
      }
    }
  }

  const stack = wasm.stackSave();
  const dimsOffset = wasm.stackAlloc(4 * dims.length);
  try {
    dims.forEach((d, index) => wasm.setValue(dimsOffset + index * ptrSize, d, ptrSize === 4 ? 'i32' : 'i64'));
    const tensor = wasm._OrtCreateTensor(
      tensorDataTypeStringToEnum(dataType),
      rawData,
      dataByteLength,
      dimsOffset,
      dims.length,
      dataLocationStringToEnum(actualLocation),
    );
    if (tensor === 0) {
      checkLastError(`Can't create tensor for input/output. session=${sessionId}, index=${index}.`);
    }
    tensorHandles.push(tensor);
  } finally {
    wasm.stackRestore(stack);
  }
};

/**
 * perform inference run
 */
export const run = async (
  sessionId: number,
  inputIndices: number[],
  inputTensors: TensorMetadata[],
  outputIndices: number[],
  outputTensors: Array<TensorMetadata | null>,
  options: InferenceSession.RunOptions,
): Promise<TensorMetadata[]> => {
  const wasm = getInstance();
  const ptrSize = wasm.PTR_SIZE;
  const session = activeSessions.get(sessionId);
  if (!session) {
    throw new Error(`cannot run inference. invalid session id: ${sessionId}`);
  }
  const sessionHandle = session[0];
  const inputNamesUTF8Encoded = session[1];
  const outputNamesUTF8Encoded = session[2];
  const ioBindingState = session[3];
  const enableGraphCapture = session[4];
  const inputOutputBound = session[5];

  const inputCount = inputIndices.length;
  const outputCount = outputIndices.length;

  let runOptionsHandle = 0;
  let runOptionsAllocs: number[] = [];

  const inputTensorHandles: number[] = [];
  const outputTensorHandles: number[] = [];
  const inputOutputAllocs: number[] = [];

  const beforeRunStack = wasm.stackSave();
  const inputValuesOffset = wasm.stackAlloc(inputCount * ptrSize);
  const inputNamesOffset = wasm.stackAlloc(inputCount * ptrSize);
  const outputValuesOffset = wasm.stackAlloc(outputCount * ptrSize);
  const outputNamesOffset = wasm.stackAlloc(outputCount * ptrSize);

  try {
    [runOptionsHandle, runOptionsAllocs] = setRunOptions(options);

    // create input tensors
    for (let i = 0; i < inputCount; i++) {
      await prepareInputOutputTensor(
        inputTensors[i],
        inputTensorHandles,
        inputOutputAllocs,
        sessionId,
        inputNamesUTF8Encoded[inputIndices[i]],
        inputIndices[i],
        enableGraphCapture,
      );
    }

    // create output tensors
    for (let i = 0; i < outputCount; i++) {
      await prepareInputOutputTensor(
        outputTensors[i],
        outputTensorHandles,
        inputOutputAllocs,
        sessionId,
        outputNamesUTF8Encoded[outputIndices[i]],
        inputCount + outputIndices[i],
        enableGraphCapture,
      );
    }

    for (let i = 0; i < inputCount; i++) {
      wasm.setValue(inputValuesOffset + i * ptrSize, inputTensorHandles[i], '*');
      wasm.setValue(inputNamesOffset + i * ptrSize, inputNamesUTF8Encoded[inputIndices[i]], '*');
    }
    for (let i = 0; i < outputCount; i++) {
      wasm.setValue(outputValuesOffset + i * ptrSize, outputTensorHandles[i], '*');
      wasm.setValue(outputNamesOffset + i * ptrSize, outputNamesUTF8Encoded[outputIndices[i]], '*');
    }

    if (!BUILD_DEFS.DISABLE_JSEP && ioBindingState && !inputOutputBound) {
      const { handle, outputPreferredLocations, outputPreferredLocationsEncoded } = ioBindingState;

      if (inputNamesUTF8Encoded.length !== inputCount) {
        throw new Error(
          `input count from feeds (${inputCount}) is expected to be always equal to model's input count (${inputNamesUTF8Encoded.length}).`,
        );
      }

      // process inputs
      for (let i = 0; i < inputCount; i++) {
        const index = inputIndices[i];
        const errorCode = await wasm._OrtBindInput(handle, inputNamesUTF8Encoded[index], inputTensorHandles[i]);
        if (errorCode !== 0) {
          checkLastError(`Can't bind input[${i}] for session=${sessionId}.`);
        }
      }

      // process pre-allocated outputs
      for (let i = 0; i < outputCount; i++) {
        const index = outputIndices[i];
        const location = outputTensors[i]?.[3]; // undefined means output is not pre-allocated.

        if (location) {
          // output is pre-allocated. bind the tensor.
          const errorCode = wasm._OrtBindOutput(handle, outputNamesUTF8Encoded[index], outputTensorHandles[i], 0);
          if (errorCode !== 0) {
            checkLastError(`Can't bind pre-allocated output[${i}] for session=${sessionId}.`);
          }
        } else {
          // output is not pre-allocated. reset preferred location.
          const errorCode = wasm._OrtBindOutput(
            handle,
            outputNamesUTF8Encoded[index],
            0,
            outputPreferredLocationsEncoded[index],
          );
          if (errorCode !== 0) {
            checkLastError(`Can't bind output[${i}] to ${outputPreferredLocations[i]} for session=${sessionId}.`);
          }
        }
      }
      activeSessions.set(sessionId, [
        sessionHandle,
        inputNamesUTF8Encoded,
        outputNamesUTF8Encoded,
        ioBindingState,
        enableGraphCapture,
        true,
      ]);
    }

    wasm.jsepOnRunStart?.(sessionHandle);
    wasm.webnnOnRunStart?.(sessionHandle);

    let errorCode: number;
    if (!BUILD_DEFS.DISABLE_JSEP && ioBindingState) {
      errorCode = await wasm._OrtRunWithBinding(
        sessionHandle,
        ioBindingState.handle,
        outputCount,
        outputValuesOffset,
        runOptionsHandle,
      );
    } else {
      errorCode = await wasm._OrtRun(
        sessionHandle,
        inputNamesOffset,
        inputValuesOffset,
        inputCount,
        outputNamesOffset,
        outputCount,
        outputValuesOffset,
        runOptionsHandle,
      );
    }

    if (errorCode !== 0) {
      checkLastError('failed to call OrtRun().');
    }

    const output: TensorMetadata[] = [];

    for (let i = 0; i < outputCount; i++) {
      const tensor = Number(wasm.getValue(outputValuesOffset + i * ptrSize, '*'));
      if (tensor === outputTensorHandles[i]) {
        // output tensor is pre-allocated. no need to copy data.
        output.push(outputTensors[i]!);
        continue;
      }

      const beforeGetTensorDataStack = wasm.stackSave();
      // stack allocate 4 pointer value
      const tensorDataOffset = wasm.stackAlloc(4 * ptrSize);

      let keepOutputTensor = false;
      let type: Tensor.Type | undefined,
        dataOffset = 0;
      try {
        const errorCode = wasm._OrtGetTensorData(
          tensor,
          tensorDataOffset,
          tensorDataOffset + ptrSize,
          tensorDataOffset + 2 * ptrSize,

          tensorDataOffset + 3 * ptrSize,
        );
        if (errorCode !== 0) {
          checkLastError(`Can't access output tensor data on index ${i}.`);
        }
        const valueType = ptrSize === 4 ? 'i32' : 'i64';
        const dataType = Number(wasm.getValue(tensorDataOffset, valueType));
        dataOffset = wasm.getValue(tensorDataOffset + ptrSize, '*');
        const dimsOffset = wasm.getValue(tensorDataOffset + ptrSize * 2, '*');
        const dimsLength = Number(wasm.getValue(tensorDataOffset + ptrSize * 3, valueType));
        const dims = [];
        for (let i = 0; i < dimsLength; i++) {
          dims.push(Number(wasm.getValue(dimsOffset + i * ptrSize, valueType)));
        }
        if (wasm._OrtFree(dimsOffset) !== 0) {
          checkLastError("Can't free memory for tensor dims.");
        }
        const size = dims.reduce((a, b) => a * b, 1);
        type = tensorDataTypeEnumToString(dataType);

        const preferredLocation = ioBindingState?.outputPreferredLocations[outputIndices[i]];

        if (type === 'string') {
          if (preferredLocation === 'gpu-buffer' || preferredLocation === 'ml-tensor') {
            throw new Error('String tensor is not supported on GPU.');
          }
          const stringData: string[] = [];
          for (let i = 0; i < size; i++) {
            const offset = wasm.getValue(dataOffset + i * ptrSize, '*');
            const nextOffset = wasm.getValue(dataOffset + (i + 1) * ptrSize, '*');
            const maxBytesToRead = i === size - 1 ? undefined : nextOffset - offset;
            stringData.push(wasm.UTF8ToString(offset, maxBytesToRead));
          }
          output.push([type, dims, stringData, 'cpu']);
        } else {
          // If a certain output's preferred location is GPU but the tensor is empty, we still need to create a CPU
          // tensor for it. There is no mapping GPU buffer for an empty tensor.
          if (preferredLocation === 'gpu-buffer' && size > 0) {
            const getBuffer = BUILD_DEFS.USE_WEBGPU_EP ? wasm.webgpuGetBuffer : wasm.jsepGetBuffer;
            if (!getBuffer) {
              throw new Error('preferredLocation "gpu-buffer" is not supported without using WebGPU.');
            }
            const gpuBuffer = getBuffer(dataOffset);
            const bufferSize = calculateTensorSizeInBytes(dataType, size);
            if (bufferSize === undefined || !isGpuBufferSupportedType(type)) {
              throw new Error(`Unsupported data type: ${type}`);
            }

            // do not release the tensor right now. it will be released when user calls tensor.dispose().
            keepOutputTensor = true;

            if (BUILD_DEFS.USE_WEBGPU_EP) {
              wasm.webgpuRegisterBuffer!(gpuBuffer, sessionId, dataOffset);
              const downloadDataFunction = wasm.webgpuCreateDownloader!(gpuBuffer, bufferSize, sessionId);
              output.push([
                type,
                dims,
                {
                  gpuBuffer,
                  download: async () => {
                    const arrayBuffer = await downloadDataFunction();
                    const data = new (tensorTypeToTypedArrayConstructor(type!))(arrayBuffer);
                    return data as Tensor.DataTypeMap[Tensor.GpuBufferDataTypes];
                  },
                  dispose: () => {
                    if (wasm._OrtReleaseTensor(tensor) !== 0) {
                      checkLastError("Can't release tensor.");
                    }
                  },
                },
                'gpu-buffer',
              ]);
            } else {
              output.push([
                type,
                dims,
                {
                  gpuBuffer,
                  download: wasm.jsepCreateDownloader!(gpuBuffer, bufferSize, type),
                  dispose: () => {
                    if (wasm._OrtReleaseTensor(tensor) !== 0) {
                      checkLastError("Can't release tensor.");
                    }
                  },
                },
                'gpu-buffer',
              ]);
            }
          } else if (preferredLocation === 'ml-tensor' && size > 0) {
            const ensureTensor = wasm.webnnEnsureTensor;
            const isInt64Supported = wasm.webnnIsInt64Supported;
            if (!ensureTensor || !isInt64Supported) {
              throw new Error('preferredLocation "ml-tensor" is not supported without using WebNN.');
            }
            const tensorSize = calculateTensorSizeInBytes(dataType, size);
            if (tensorSize === undefined || !isMLTensorSupportedType(type)) {
              throw new Error(`Unsupported data type: ${type}`);
            }
            if (type === 'int64' && !isInt64Supported(sessionId)) {
              throw new Error(
                `preferredLocation "ml-tensor" for int64 output is not supported by current WebNN Context.`,
              );
            }

            // If the graph has been partitioned, the output tensor may have not been created. For this reason, we use
            // ensureTensor to get/create the MLTensor. In which case, we don't need to copy the data if a new tensor
            // has been created.
            const mlTensor = await ensureTensor(sessionId, dataOffset, dataType, dims, false);

            // do not release the tensor right now. it will be released when user calls tensor.dispose().
            keepOutputTensor = true;

            output.push([
              type,
              dims,
              {
                mlTensor,
                download: wasm.webnnCreateMLTensorDownloader!(dataOffset, type),
                dispose: () => {
                  wasm.webnnReleaseTensorId!(dataOffset);
                  wasm._OrtReleaseTensor(tensor);
                },
              },
              'ml-tensor',
            ]);
          } else {
            const typedArrayConstructor = tensorTypeToTypedArrayConstructor(type);
            const data = new typedArrayConstructor(size);
            new Uint8Array(data.buffer, data.byteOffset, data.byteLength).set(
              wasm.HEAPU8.subarray(dataOffset, dataOffset + data.byteLength),
            );
            output.push([type, dims, data, 'cpu']);
          }
        }
      } finally {
        wasm.stackRestore(beforeGetTensorDataStack);
        if (type === 'string' && dataOffset) {
          wasm._free(dataOffset);
        }
        if (!keepOutputTensor) {
          wasm._OrtReleaseTensor(tensor);
        }
        wasm.webnnOnRunEnd?.(sessionHandle);
      }
    }

    if (ioBindingState && !enableGraphCapture) {
      if (wasm._OrtClearBoundOutputs(ioBindingState.handle) !== 0) {
        checkLastError("Can't clear bound outputs.");
      }
      activeSessions.set(sessionId, [
        sessionHandle,
        inputNamesUTF8Encoded,
        outputNamesUTF8Encoded,
        ioBindingState,
        enableGraphCapture,
        false,
      ]);
    }
    return output;
  } finally {
    wasm.stackRestore(beforeRunStack);

    if (BUILD_DEFS.USE_WEBGPU_EP) {
      inputTensors.forEach((t) => {
        if (t && t[3] === 'gpu-buffer') {
          wasm.webgpuUnregisterBuffer!(t[2].gpuBuffer);
        }
      });
      outputTensors.forEach((t) => {
        if (t && t[3] === 'gpu-buffer') {
          wasm.webgpuUnregisterBuffer!(t[2].gpuBuffer);
        }
      });
    }
    inputTensorHandles.forEach((v) => wasm._OrtReleaseTensor(v));
    outputTensorHandles.forEach((v) => wasm._OrtReleaseTensor(v));
    inputOutputAllocs.forEach((p) => wasm._free(p));

    if (runOptionsHandle !== 0) {
      wasm._OrtReleaseRunOptions(runOptionsHandle);
    }
    runOptionsAllocs.forEach((p) => wasm._free(p));
  }
};

/**
 * end profiling
 */
export const endProfiling = (sessionId: number): void => {
  const wasm = getInstance();
  const session = activeSessions.get(sessionId);
  if (!session) {
    throw new Error('invalid session id');
  }
  const sessionHandle = session[0];

  // profile file name is not used yet, but it must be freed.
  const profileFileName = wasm._OrtEndProfiling(sessionHandle);
  if (profileFileName === 0) {
    checkLastError("Can't get an profile file name.");
  }
  wasm._OrtFree(profileFileName);
};

export const extractTransferableBuffers = (tensors: readonly SerializableTensorMetadata[]): ArrayBufferLike[] => {
  const buffers: ArrayBufferLike[] = [];
  for (const tensor of tensors) {
    const data = tensor[2];
    if (!Array.isArray(data) && 'buffer' in data) {
      buffers.push(data.buffer);
    }
  }
  return buffers;
};
