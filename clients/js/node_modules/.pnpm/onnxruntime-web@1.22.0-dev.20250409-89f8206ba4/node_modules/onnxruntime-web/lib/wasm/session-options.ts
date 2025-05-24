// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import type { InferenceSession } from 'onnxruntime-common';

import { getInstance } from './wasm-factory';
import { allocWasmString, checkLastError, iterateExtraOptions } from './wasm-utils';

const getGraphOptimzationLevel = (graphOptimizationLevel: string | unknown): number => {
  switch (graphOptimizationLevel) {
    case 'disabled':
      return 0;
    case 'basic':
      return 1;
    case 'extended':
      return 2;
    case 'all':
      return 99;
    default:
      throw new Error(`unsupported graph optimization level: ${graphOptimizationLevel}`);
  }
};

const getExecutionMode = (executionMode: 'sequential' | 'parallel'): number => {
  switch (executionMode) {
    case 'sequential':
      return 0;
    case 'parallel':
      return 1;
    default:
      throw new Error(`unsupported execution mode: ${executionMode}`);
  }
};

const appendDefaultOptions = (options: InferenceSession.SessionOptions): void => {
  if (!options.extra) {
    options.extra = {};
  }
  if (!options.extra.session) {
    options.extra.session = {};
  }
  const session = options.extra.session as Record<string, string>;
  if (!session.use_ort_model_bytes_directly) {
    // eslint-disable-next-line camelcase
    session.use_ort_model_bytes_directly = '1';
  }

  // if using JSEP with WebGPU, always disable memory pattern
  if (
    options.executionProviders &&
    options.executionProviders.some((ep) => (typeof ep === 'string' ? ep : ep.name) === 'webgpu')
  ) {
    options.enableMemPattern = false;
  }
};

const appendSessionConfig = (sessionOptionsHandle: number, key: string, value: string, allocs: number[]): void => {
  const keyDataOffset = allocWasmString(key, allocs);
  const valueDataOffset = allocWasmString(value, allocs);
  if (getInstance()._OrtAddSessionConfigEntry(sessionOptionsHandle, keyDataOffset, valueDataOffset) !== 0) {
    checkLastError(`Can't set a session config entry: ${key} - ${value}.`);
  }
};

const appendEpOption = (epOptions: Array<[number, number]>, key: string, value: string, allocs: number[]): void => {
  const keyDataOffset = allocWasmString(key, allocs);
  const valueDataOffset = allocWasmString(value, allocs);
  epOptions.push([keyDataOffset, valueDataOffset]);
};

const setExecutionProviders = async (
  sessionOptionsHandle: number,
  executionProviders: readonly InferenceSession.ExecutionProviderConfig[],
  allocs: number[],
): Promise<void> => {
  for (const ep of executionProviders) {
    let epName = typeof ep === 'string' ? ep : ep.name;
    const epOptions: Array<[number, number]> = [];

    // check EP name
    switch (epName) {
      case 'webnn':
        epName = 'WEBNN';
        if (typeof ep !== 'string') {
          const webnnOptions = ep as InferenceSession.WebNNExecutionProviderOption;
          // const context = (webnnOptions as InferenceSession.WebNNOptionsWithMLContext)?.context;
          const deviceType = (webnnOptions as InferenceSession.WebNNContextOptions)?.deviceType;
          if (deviceType) {
            appendSessionConfig(sessionOptionsHandle, 'deviceType', deviceType, allocs);
          }
        }
        break;
      case 'webgpu':
        if (BUILD_DEFS.USE_WEBGPU_EP) {
          epName = 'WebGPU';
          let customDevice: GPUDevice | undefined;

          if (typeof ep !== 'string') {
            const customOptions = ep as unknown as { device: GPUDevice };
            if (customOptions.device) {
              if (typeof GPUDevice !== 'undefined' && customOptions.device instanceof GPUDevice) {
                customDevice = customOptions.device;
              } else {
                throw new Error('Invalid GPU device set in WebGPU EP options.');
              }
            }

            // TODO: handle more options
          }

          const info = getInstance().webgpuRegisterDevice!(customDevice);
          if (info) {
            const [deviceId, instanceHandle, deviceHandle] = info;
            appendEpOption(epOptions, 'deviceId', deviceId.toString(), allocs);
            appendEpOption(epOptions, 'webgpuInstance', instanceHandle.toString(), allocs);
            appendEpOption(epOptions, 'webgpuDevice', deviceHandle.toString(), allocs);
          }
        } else {
          epName = 'JS';
          if (typeof ep !== 'string') {
            const webgpuOptions = ep as InferenceSession.WebGpuExecutionProviderOption;
            if (webgpuOptions?.preferredLayout) {
              if (webgpuOptions.preferredLayout !== 'NCHW' && webgpuOptions.preferredLayout !== 'NHWC') {
                throw new Error(`preferredLayout must be either 'NCHW' or 'NHWC': ${webgpuOptions.preferredLayout}`);
              }
              appendSessionConfig(sessionOptionsHandle, 'preferredLayout', webgpuOptions.preferredLayout, allocs);
            }
          }
        }
        break;
      case 'wasm':
      case 'cpu':
        continue;
      default:
        throw new Error(`not supported execution provider: ${epName}`);
    }

    const epNameDataOffset = allocWasmString(epName, allocs);
    const epOptionsCount = epOptions.length;
    let keysOffset = 0;
    let valuesOffset = 0;
    if (epOptionsCount > 0) {
      keysOffset = getInstance()._malloc(epOptionsCount * getInstance().PTR_SIZE);
      allocs.push(keysOffset);
      valuesOffset = getInstance()._malloc(epOptionsCount * getInstance().PTR_SIZE);
      allocs.push(valuesOffset);
      for (let i = 0; i < epOptionsCount; i++) {
        getInstance().setValue(keysOffset + i * getInstance().PTR_SIZE, epOptions[i][0], '*');
        getInstance().setValue(valuesOffset + i * getInstance().PTR_SIZE, epOptions[i][1], '*');
      }
    }
    if (
      (await getInstance()._OrtAppendExecutionProvider(
        sessionOptionsHandle,
        epNameDataOffset,
        keysOffset,
        valuesOffset,
        epOptionsCount,
      )) !== 0
    ) {
      checkLastError(`Can't append execution provider: ${epName}.`);
    }
  }
};

export const setSessionOptions = async (options?: InferenceSession.SessionOptions): Promise<[number, number[]]> => {
  const wasm = getInstance();
  let sessionOptionsHandle = 0;
  const allocs: number[] = [];

  const sessionOptions: InferenceSession.SessionOptions = options || {};
  appendDefaultOptions(sessionOptions);

  try {
    const graphOptimizationLevel = getGraphOptimzationLevel(sessionOptions.graphOptimizationLevel ?? 'all');
    const executionMode = getExecutionMode(sessionOptions.executionMode ?? 'sequential');
    const logIdDataOffset =
      typeof sessionOptions.logId === 'string' ? allocWasmString(sessionOptions.logId, allocs) : 0;

    const logSeverityLevel = sessionOptions.logSeverityLevel ?? 2; // Default to 2 - warning
    if (!Number.isInteger(logSeverityLevel) || logSeverityLevel < 0 || logSeverityLevel > 4) {
      throw new Error(`log serverity level is not valid: ${logSeverityLevel}`);
    }

    const logVerbosityLevel = sessionOptions.logVerbosityLevel ?? 0; // Default to 0 - verbose
    if (!Number.isInteger(logVerbosityLevel) || logVerbosityLevel < 0 || logVerbosityLevel > 4) {
      throw new Error(`log verbosity level is not valid: ${logVerbosityLevel}`);
    }

    const optimizedModelFilePathOffset =
      typeof sessionOptions.optimizedModelFilePath === 'string'
        ? allocWasmString(sessionOptions.optimizedModelFilePath, allocs)
        : 0;

    sessionOptionsHandle = wasm._OrtCreateSessionOptions(
      graphOptimizationLevel,
      !!sessionOptions.enableCpuMemArena,
      !!sessionOptions.enableMemPattern,
      executionMode,
      !!sessionOptions.enableProfiling,
      0,
      logIdDataOffset,
      logSeverityLevel,
      logVerbosityLevel,
      optimizedModelFilePathOffset,
    );
    if (sessionOptionsHandle === 0) {
      checkLastError("Can't create session options.");
    }

    if (sessionOptions.executionProviders) {
      await setExecutionProviders(sessionOptionsHandle, sessionOptions.executionProviders, allocs);
    }

    if (sessionOptions.enableGraphCapture !== undefined) {
      if (typeof sessionOptions.enableGraphCapture !== 'boolean') {
        throw new Error(`enableGraphCapture must be a boolean value: ${sessionOptions.enableGraphCapture}`);
      }
      appendSessionConfig(
        sessionOptionsHandle,
        'enableGraphCapture',
        sessionOptions.enableGraphCapture.toString(),
        allocs,
      );
    }

    if (sessionOptions.freeDimensionOverrides) {
      for (const [name, value] of Object.entries(sessionOptions.freeDimensionOverrides)) {
        if (typeof name !== 'string') {
          throw new Error(`free dimension override name must be a string: ${name}`);
        }
        if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
          throw new Error(`free dimension override value must be a non-negative integer: ${value}`);
        }
        const nameOffset = allocWasmString(name, allocs);
        if (wasm._OrtAddFreeDimensionOverride(sessionOptionsHandle, nameOffset, value) !== 0) {
          checkLastError(`Can't set a free dimension override: ${name} - ${value}.`);
        }
      }
    }

    if (sessionOptions.extra !== undefined) {
      iterateExtraOptions(sessionOptions.extra, '', new WeakSet<Record<string, unknown>>(), (key, value) => {
        appendSessionConfig(sessionOptionsHandle, key, value, allocs);
      });
    }

    return [sessionOptionsHandle, allocs];
  } catch (e) {
    if (sessionOptionsHandle !== 0) {
      if (wasm._OrtReleaseSessionOptions(sessionOptionsHandle) !== 0) {
        checkLastError("Can't release session options.");
      }
    }
    allocs.forEach((alloc) => wasm._free(alloc));
    throw e;
  }
};
