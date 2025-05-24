// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Env } from './env.js';
import { version } from './version.js';

type LogLevelType = Env['logLevel'];

let logLevelValue: Required<LogLevelType> = 'warning';

export const env: Env = {
  wasm: {} as Env.WebAssemblyFlags,
  webgl: {} as Env.WebGLFlags,
  webgpu: {} as Env.WebGpuFlags,
  versions: { common: version },

  set logLevel(value: LogLevelType) {
    if (value === undefined) {
      return;
    }
    if (typeof value !== 'string' || ['verbose', 'info', 'warning', 'error', 'fatal'].indexOf(value) === -1) {
      throw new Error(`Unsupported logging level: ${value}`);
    }
    logLevelValue = value;
  },
  get logLevel(): Required<LogLevelType> {
    return logLevelValue;
  },
};

// set property 'logLevel' so that they can be correctly transferred to worker by `postMessage()`.
Object.defineProperty(env, 'logLevel', { enumerable: true });
