// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { resolveBackendAndExecutionProviders } from './backend-impl.js';
import { InferenceSessionHandler } from './backend.js';
import { InferenceSession as InferenceSessionInterface } from './inference-session.js';
import { OnnxValue } from './onnx-value.js';
import { Tensor } from './tensor.js';
import { TRACE_FUNC_BEGIN, TRACE_FUNC_END } from './trace.js';

type SessionOptions = InferenceSessionInterface.SessionOptions;
type RunOptions = InferenceSessionInterface.RunOptions;
type FeedsType = InferenceSessionInterface.FeedsType;
type FetchesType = InferenceSessionInterface.FetchesType;
type ReturnType = InferenceSessionInterface.ReturnType;

export class InferenceSession implements InferenceSessionInterface {
  private constructor(handler: InferenceSessionHandler) {
    this.handler = handler;
  }
  run(feeds: FeedsType, options?: RunOptions): Promise<ReturnType>;
  run(feeds: FeedsType, fetches: FetchesType, options?: RunOptions): Promise<ReturnType>;
  async run(feeds: FeedsType, arg1?: FetchesType | RunOptions, arg2?: RunOptions): Promise<ReturnType> {
    TRACE_FUNC_BEGIN();
    const fetches: { [name: string]: OnnxValue | null } = {};
    let options: RunOptions = {};
    // check inputs
    if (typeof feeds !== 'object' || feeds === null || feeds instanceof Tensor || Array.isArray(feeds)) {
      throw new TypeError(
        "'feeds' must be an object that use input names as keys and OnnxValue as corresponding values.",
      );
    }

    let isFetchesEmpty = true;
    // determine which override is being used
    if (typeof arg1 === 'object') {
      if (arg1 === null) {
        throw new TypeError('Unexpected argument[1]: cannot be null.');
      }
      if (arg1 instanceof Tensor) {
        throw new TypeError("'fetches' cannot be a Tensor");
      }

      if (Array.isArray(arg1)) {
        if (arg1.length === 0) {
          throw new TypeError("'fetches' cannot be an empty array.");
        }
        isFetchesEmpty = false;
        // output names
        for (const name of arg1) {
          if (typeof name !== 'string') {
            throw new TypeError("'fetches' must be a string array or an object.");
          }
          if (this.outputNames.indexOf(name) === -1) {
            throw new RangeError(`'fetches' contains invalid output name: ${name}.`);
          }
          fetches[name] = null;
        }

        if (typeof arg2 === 'object' && arg2 !== null) {
          options = arg2;
        } else if (typeof arg2 !== 'undefined') {
          throw new TypeError("'options' must be an object.");
        }
      } else {
        // decide whether arg1 is fetches or options
        // if any output name is present and its value is valid OnnxValue, we consider it fetches
        let isFetches = false;
        const arg1Keys = Object.getOwnPropertyNames(arg1);
        for (const name of this.outputNames) {
          if (arg1Keys.indexOf(name) !== -1) {
            const v = (arg1 as InferenceSessionInterface.NullableOnnxValueMapType)[name];
            if (v === null || v instanceof Tensor) {
              isFetches = true;
              isFetchesEmpty = false;
              fetches[name] = v;
            }
          }
        }

        if (isFetches) {
          if (typeof arg2 === 'object' && arg2 !== null) {
            options = arg2;
          } else if (typeof arg2 !== 'undefined') {
            throw new TypeError("'options' must be an object.");
          }
        } else {
          options = arg1 as RunOptions;
        }
      }
    } else if (typeof arg1 !== 'undefined') {
      throw new TypeError("Unexpected argument[1]: must be 'fetches' or 'options'.");
    }

    // check if all inputs are in feed
    for (const name of this.inputNames) {
      if (typeof feeds[name] === 'undefined') {
        throw new Error(`input '${name}' is missing in 'feeds'.`);
      }
    }

    // if no fetches is specified, we use the full output names list
    if (isFetchesEmpty) {
      for (const name of this.outputNames) {
        fetches[name] = null;
      }
    }

    // feeds, fetches and options are prepared

    const results = await this.handler.run(feeds, fetches, options);
    const returnValue: { [name: string]: OnnxValue } = {};
    for (const key in results) {
      if (Object.hasOwnProperty.call(results, key)) {
        const result = results[key];
        if (result instanceof Tensor) {
          returnValue[key] = result;
        } else {
          returnValue[key] = new Tensor(result.type, result.data, result.dims);
        }
      }
    }
    TRACE_FUNC_END();
    return returnValue;
  }

  async release(): Promise<void> {
    return this.handler.dispose();
  }

  static create(path: string, options?: SessionOptions): Promise<InferenceSessionInterface>;
  static create(buffer: ArrayBufferLike, options?: SessionOptions): Promise<InferenceSessionInterface>;
  static create(
    buffer: ArrayBufferLike,
    byteOffset: number,
    byteLength?: number,
    options?: SessionOptions,
  ): Promise<InferenceSessionInterface>;
  static create(buffer: Uint8Array, options?: SessionOptions): Promise<InferenceSessionInterface>;
  static async create(
    arg0: string | ArrayBufferLike | Uint8Array,
    arg1?: SessionOptions | number,
    arg2?: number,
    arg3?: SessionOptions,
  ): Promise<InferenceSessionInterface> {
    TRACE_FUNC_BEGIN();
    // either load from a file or buffer
    let filePathOrUint8Array: string | Uint8Array;
    let options: SessionOptions = {};

    if (typeof arg0 === 'string') {
      filePathOrUint8Array = arg0;
      if (typeof arg1 === 'object' && arg1 !== null) {
        options = arg1;
      } else if (typeof arg1 !== 'undefined') {
        throw new TypeError("'options' must be an object.");
      }
    } else if (arg0 instanceof Uint8Array) {
      filePathOrUint8Array = arg0;
      if (typeof arg1 === 'object' && arg1 !== null) {
        options = arg1;
      } else if (typeof arg1 !== 'undefined') {
        throw new TypeError("'options' must be an object.");
      }
    } else if (
      arg0 instanceof ArrayBuffer ||
      (typeof SharedArrayBuffer !== 'undefined' && arg0 instanceof SharedArrayBuffer)
    ) {
      const buffer = arg0;
      let byteOffset = 0;
      let byteLength = arg0.byteLength;
      if (typeof arg1 === 'object' && arg1 !== null) {
        options = arg1;
      } else if (typeof arg1 === 'number') {
        byteOffset = arg1;
        if (!Number.isSafeInteger(byteOffset)) {
          throw new RangeError("'byteOffset' must be an integer.");
        }
        if (byteOffset < 0 || byteOffset >= buffer.byteLength) {
          throw new RangeError(`'byteOffset' is out of range [0, ${buffer.byteLength}).`);
        }
        byteLength = arg0.byteLength - byteOffset;
        if (typeof arg2 === 'number') {
          byteLength = arg2;
          if (!Number.isSafeInteger(byteLength)) {
            throw new RangeError("'byteLength' must be an integer.");
          }
          if (byteLength <= 0 || byteOffset + byteLength > buffer.byteLength) {
            throw new RangeError(`'byteLength' is out of range (0, ${buffer.byteLength - byteOffset}].`);
          }
          if (typeof arg3 === 'object' && arg3 !== null) {
            options = arg3;
          } else if (typeof arg3 !== 'undefined') {
            throw new TypeError("'options' must be an object.");
          }
        } else if (typeof arg2 !== 'undefined') {
          throw new TypeError("'byteLength' must be a number.");
        }
      } else if (typeof arg1 !== 'undefined') {
        throw new TypeError("'options' must be an object.");
      }
      filePathOrUint8Array = new Uint8Array(buffer, byteOffset, byteLength);
    } else {
      throw new TypeError("Unexpected argument[0]: must be 'path' or 'buffer'.");
    }

    // resolve backend, update session options with validated EPs, and create session handler
    const [backend, optionsWithValidatedEPs] = await resolveBackendAndExecutionProviders(options);
    const handler = await backend.createInferenceSessionHandler(filePathOrUint8Array, optionsWithValidatedEPs);
    TRACE_FUNC_END();
    return new InferenceSession(handler);
  }

  startProfiling(): void {
    this.handler.startProfiling();
  }
  endProfiling(): void {
    this.handler.endProfiling();
  }

  get inputNames(): readonly string[] {
    return this.handler.inputNames;
  }
  get outputNames(): readonly string[] {
    return this.handler.outputNames;
  }

  private handler: InferenceSessionHandler;
}
