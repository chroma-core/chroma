// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from './env-impl.js';

/**
 * @ignore
 */
export const TRACE = (deviceType: string, label: string) => {
  if (typeof env.trace === 'undefined' ? !env.wasm.trace : !env.trace) {
    return;
  }
  // eslint-disable-next-line no-console
  console.timeStamp(`${deviceType}::ORT::${label}`);
};

const TRACE_FUNC = (msg: string, extraMsg?: string) => {
  const stack = new Error().stack?.split(/\r\n|\r|\n/g) || [];
  let hasTraceFunc = false;
  for (let i = 0; i < stack.length; i++) {
    if (hasTraceFunc && !stack[i].includes('TRACE_FUNC')) {
      let label = `FUNC_${msg}::${stack[i].trim().split(' ')[1]}`;
      if (extraMsg) {
        label += `::${extraMsg}`;
      }
      TRACE('CPU', label);
      return;
    }
    if (stack[i].includes('TRACE_FUNC')) {
      hasTraceFunc = true;
    }
  }
};

/**
 * @ignore
 */
export const TRACE_FUNC_BEGIN = (extraMsg?: string) => {
  if (typeof env.trace === 'undefined' ? !env.wasm.trace : !env.trace) {
    return;
  }
  TRACE_FUNC('BEGIN', extraMsg);
};

/**
 * @ignore
 */
export const TRACE_FUNC_END = (extraMsg?: string) => {
  if (typeof env.trace === 'undefined' ? !env.wasm.trace : !env.trace) {
    return;
  }
  TRACE_FUNC('END', extraMsg);
};
