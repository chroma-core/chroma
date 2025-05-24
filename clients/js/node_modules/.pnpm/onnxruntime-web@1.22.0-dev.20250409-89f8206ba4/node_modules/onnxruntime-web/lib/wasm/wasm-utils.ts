// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { getInstance } from './wasm-factory';

export const allocWasmString = (data: string, allocs: number[]): number => {
  const wasm = getInstance();

  const dataLength = wasm.lengthBytesUTF8(data) + 1;
  const dataOffset = wasm._malloc(dataLength);
  wasm.stringToUTF8(data, dataOffset, dataLength);
  allocs.push(dataOffset);

  return dataOffset;
};

interface ExtraOptionsHandler {
  (name: string, value: string): void;
}

export const iterateExtraOptions = (
  options: Record<string, unknown>,
  prefix: string,
  seen: WeakSet<Record<string, unknown>>,
  handler: ExtraOptionsHandler,
): void => {
  if (typeof options == 'object' && options !== null) {
    if (seen.has(options)) {
      throw new Error('Circular reference in options');
    } else {
      seen.add(options);
    }
  }

  Object.entries(options).forEach(([key, value]) => {
    const name = prefix ? prefix + key : key;
    if (typeof value === 'object') {
      iterateExtraOptions(value as Record<string, unknown>, name + '.', seen, handler);
    } else if (typeof value === 'string' || typeof value === 'number') {
      handler(name, value.toString());
    } else if (typeof value === 'boolean') {
      handler(name, value ? '1' : '0');
    } else {
      throw new Error(`Can't handle extra config type: ${typeof value}`);
    }
  });
};

/**
 * check web assembly API's last error and throw error if any error occurred.
 * @param message a message used when an error occurred.
 */
export const checkLastError = (message: string): void => {
  const wasm = getInstance();

  const stack = wasm.stackSave();
  try {
    const ptrSize = wasm.PTR_SIZE;
    const paramsOffset = wasm.stackAlloc(2 * ptrSize);
    wasm._OrtGetLastError(paramsOffset, paramsOffset + ptrSize);
    const errorCode = Number(wasm.getValue(paramsOffset, ptrSize === 4 ? 'i32' : 'i64'));
    const errorMessagePointer = wasm.getValue(paramsOffset + ptrSize, '*');
    const errorMessage = errorMessagePointer ? wasm.UTF8ToString(errorMessagePointer) : '';
    throw new Error(`${message} ERROR_CODE: ${errorCode}, ERROR_MESSAGE: ${errorMessage}`);
  } finally {
    wasm.stackRestore(stack);
  }
};
