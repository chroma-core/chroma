// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { WebNNBackend } from '../backend-webnn';
import { LOG_DEBUG } from '../log';

// WebNN API currently does not have a TypeScript definition file. This file is a workaround with types generated from
// WebNN API specification.
// https://github.com/webmachinelearning/webnn/issues/677
/// <reference path="webnn.d.ts" />

// Convert BigInt64Array buffer data to Int32Array buffer data.
export const convertInt64ToInt32 = (data: Uint8Array, returnUint8 = true): Uint8Array | Int32Array => {
  // Make sure it is a multiple of 8 bytes (BigInt64Array).
  if (data.byteLength % 8 !== 0) {
    throw new Error('Invalid Uint8Array length - must be a multiple of 8 (BigInt).');
  }

  // Convert Uint8Array to BigInt64Array.
  const numElements = data.byteLength / 8;
  const bigInt64Array = new BigInt64Array(data.buffer, data.byteOffset, numElements);

  // Convert BigInt64Array to Int32Array (same number of elements).
  const int32Array = new Int32Array(numElements);

  for (let i = 0; i < numElements; i++) {
    const value = bigInt64Array[i];

    // Check for overflow.
    if (value > 2147483647n || value < -2147483648n) {
      throw new Error(`Overflow occurred when converting BigInt to Int32 at index ${i}: ${value}`);
    }

    int32Array[i] = Number(value);
  }

  // Return based on the requested format.
  return returnUint8 ? new Uint8Array(int32Array.buffer) : int32Array;
};

// Convert Int32Array buffer data to BigInt64Array buffer data.
const convertInt32ToInt64 = (data: Uint8Array, returnUint8 = true): Uint8Array | BigInt64Array => {
  // Make sure it is a multiple of 4 bytes (Int32Array).
  if (data.byteLength % 4 !== 0) {
    throw new Error('Invalid Uint8Array length - must be a multiple of 4 (Int32).');
  }

  // Convert Uint8Array to Int32Array.
  const numElements = data.byteLength / 4;
  const int32Array = new Int32Array(data.buffer, data.byteOffset, numElements);

  // Convert Int32Array to BigInt64Array (same number of elements).
  const bigInt64Array = BigInt64Array.from(int32Array, BigInt);

  // Return based on the requested format.
  return returnUint8 ? new Uint8Array(bigInt64Array.buffer) : bigInt64Array;
};

export type TensorId = number;

/**
 * Manages TensorId to MLTensor mapping.
 */
export interface TensorManager {
  /**
   * Reserve a new TensorId.
   */
  reserveTensorId(): TensorId;
  /**
   * Release a TensorId.
   */
  releaseTensorId(tensorId: TensorId): void;
  /**
   * Ensure a MLTensor is created for the TensorId.
   */
  ensureTensor(
    sessionId: number,
    tensorId: TensorId,
    dataType: MLOperandDataType,
    shape: readonly number[],
    copyOld: boolean,
  ): Promise<MLTensor>;
  /**
   * Upload data to a MLTensor.
   */
  upload(tensorId: TensorId, data: Uint8Array): void;
  /**
   * Download data from a MLTensor.
   */
  download(tensorId: TensorId): Promise<ArrayBuffer>;
  download(tensorId: TensorId, dstTensor: ArrayBufferView | ArrayBuffer): Promise<undefined>;
  /**
   * Release all tensors for a given session.
   */
  releaseTensorsForSession(session: number): void;
  /**
   * Register an externally created MLTensor with a given session id and return a TensorId.
   */
  registerTensor(sessionId: number, mlTensor: MLTensor, dataType: MLOperandDataType, shape: number[]): TensorId;
}

let tensorGuid = 1;
const createNewTensorId = (): TensorId => tensorGuid++;

/**
 * Map from MLOperandDataType to size in bits. Using bits instead of bytes to avoid possible precision loss on int4 and uint4.
 */
const webnnDataTypeToSize = new Map<MLOperandDataType, number>([
  ['float32', 32],
  ['float16', 16],
  ['int32', 32],
  ['uint32', 32],
  ['int64', 64],
  ['uint64', 64],
  ['int8', 8],
  ['uint8', 8],
  ['int4', 4],
  ['uint4', 4],
]);

/**
 * Calculate the byte length of a tensor with the given data type and shape.
 */
const calculateByteLength = (dataType: MLOperandDataType, shape: readonly number[]): number => {
  const size = webnnDataTypeToSize.get(dataType);
  if (!size) {
    throw new Error('Unsupported data type.');
  }
  return shape.length > 0 ? Math.ceil((shape.reduce((a, b) => a * b) * size) / 8) : 0;
};

/**
 * TensorWrapper wraps an MLTensor and provides a way to track the last session that used it.
 */
class TensorWrapper {
  // The id of the last session that used this tensor.
  public sessionId: number;
  // This flag is used to indicate whether we should convert data from int64 to int32.
  public shouldConvertInt64toInt32 = false;
  public isInt64ToInt32Converted = false;

  private mlContext: MLContext;
  private mlTensor: MLTensor;
  private dataType: MLOperandDataType;
  private tensorShape: readonly number[];

  constructor(descriptor: {
    sessionId: number;
    context: MLContext;
    tensor: MLTensor;
    dataType: MLOperandDataType;
    shape: readonly number[];
    shouldConvertInt64toInt32?: boolean;
  }) {
    const { sessionId, context, tensor, dataType, shape, shouldConvertInt64toInt32 = false } = descriptor;
    this.sessionId = sessionId;
    this.mlContext = context;
    this.mlTensor = tensor;
    this.dataType = dataType;
    this.tensorShape = shape;
    this.shouldConvertInt64toInt32 = shouldConvertInt64toInt32;
  }

  public get tensor(): MLTensor {
    return this.mlTensor;
  }

  public get type(): MLOperandDataType {
    return this.dataType;
  }

  public get shape(): readonly number[] {
    return this.tensorShape;
  }

  public get byteLength(): number {
    return calculateByteLength(this.dataType, this.tensorShape);
  }

  public destroy(): void {
    LOG_DEBUG('verbose', () => '[WebNN] TensorWrapper.destroy');
    this.mlTensor.destroy();
  }

  public write(data: Uint8Array): void {
    this.mlContext.writeTensor(this.mlTensor, data);
  }

  public async read(shouldConvertInt32ToInt64?: boolean): Promise<ArrayBuffer>;
  public async read(
    shouldConvertInt32ToInt64?: boolean,
    dstBuffer?: ArrayBufferView | ArrayBuffer,
  ): Promise<ArrayBuffer | undefined>;
  public async read(
    shouldConvertInt32ToInt64?: boolean,
    dstBuffer?: ArrayBufferView | ArrayBuffer,
  ): Promise<ArrayBuffer | undefined> {
    if (shouldConvertInt32ToInt64) {
      // This was an int64 data as saved as int32 as workaround, we need to read it as int64.
      const data = await this.mlContext.readTensor(this.mlTensor);
      const int64Data = convertInt32ToInt64(new Uint8Array(data)) as Uint8Array;

      if (dstBuffer) {
        const targetBuffer =
          dstBuffer instanceof ArrayBuffer
            ? new Uint8Array(dstBuffer)
            : new Uint8Array(dstBuffer.buffer, dstBuffer.byteOffset, dstBuffer.byteLength);
        targetBuffer.set(int64Data);
        return undefined;
      } else {
        return int64Data.buffer;
      }
    } else {
      return dstBuffer ? this.mlContext.readTensor(this.mlTensor, dstBuffer) : this.mlContext.readTensor(this.mlTensor);
    }
  }

  public canReuseTensor(context: MLContext, dataType: MLOperandDataType, shape: readonly number[]): boolean {
    return (
      this.mlContext === context &&
      this.dataType === dataType &&
      this.tensorShape.length === shape.length &&
      this.tensorShape.every((v, i) => v === shape[i])
    );
  }

  public setIsInt64ToInt32Converted(isConverted: boolean): void {
    this.isInt64ToInt32Converted = isConverted;
  }
}

/**
 * TensorTracker tracks the MLTensor and pending upload data.
 *
 * We need to track the MLTensor and pending upload data because we delay the creation of MLTensor until
 * we know the data type and shape. This is because WebNN only support creating MLTensors with dataTypes and shape.
 */
class TensorIdTracker {
  private activeUpload?: Uint8Array;

  constructor(
    private tensorManager: TensorManagerImpl,
    private wrapper?: TensorWrapper,
  ) {}

  public get tensorWrapper(): TensorWrapper | undefined {
    return this.wrapper;
  }

  public releaseTensor(): void {
    if (this.tensorWrapper) {
      this.tensorManager.releaseTensor(this.tensorWrapper);
      this.wrapper = undefined;
    }
  }

  public async ensureTensor(
    sessionId: number,
    dataType: MLOperandDataType,
    shape: readonly number[],
    copyOld: boolean,
  ): Promise<MLTensor> {
    let newDataType = dataType;
    const context = this.tensorManager.getMLContext(sessionId);
    // If the data type is int64 and the context does not support int64, we need to convert it to int32.
    const shouldConvertInt64toInt32 =
      newDataType === 'int64' && !context.opSupportLimits().input.dataTypes.includes('int64');
    if (shouldConvertInt64toInt32) {
      newDataType = 'int32';
      LOG_DEBUG('verbose', () => `[WebNN] TensorIdTracker.ensureTensor: convert dataType from int64 to int32`);
    }

    if (this.wrapper) {
      if (this.wrapper.canReuseTensor(context, newDataType, shape)) {
        return this.wrapper.tensor;
      } else {
        if (copyOld) {
          if (this.wrapper.byteLength !== calculateByteLength(newDataType, shape)) {
            throw new Error('Unable to copy data to tensor with different size.');
          }
          this.activeUpload = new Uint8Array(await this.wrapper.read());
        }
        this.tensorManager.releaseTensor(this.wrapper);
      }
    }

    // eslint-disable-next-line no-bitwise
    const usage = typeof MLTensorUsage == 'undefined' ? undefined : MLTensorUsage.READ | MLTensorUsage.WRITE;
    this.wrapper = await this.tensorManager.getCachedTensor(
      sessionId,
      newDataType,
      shape,
      usage,
      true,
      true,
      shouldConvertInt64toInt32,
    );

    if (copyOld && this.activeUpload) {
      // We don't need to convert the old int64 data to int32,
      // because it has been converted when it was uploaded.
      this.wrapper.write(this.activeUpload);
      this.activeUpload = undefined;
    }

    return this.wrapper.tensor;
  }

  public upload(data: Uint8Array): void {
    let newData = data;
    if (this.wrapper) {
      if (this.wrapper.shouldConvertInt64toInt32) {
        // Convert int64 to int32.
        newData = convertInt64ToInt32(data, true) as Uint8Array;
        this.wrapper.setIsInt64ToInt32Converted(true);
      }
      if (newData.byteLength === this.wrapper.byteLength) {
        this.wrapper.write(newData);
        return;
      } else {
        LOG_DEBUG('verbose', () => 'Data size does not match tensor size. Releasing tensor.');
        this.releaseTensor();
      }
    }

    if (this.activeUpload) {
      this.activeUpload.set(newData);
    } else {
      this.activeUpload = new Uint8Array(newData);
    }
  }

  public async download(dstBuffer?: ArrayBufferView | ArrayBuffer): Promise<ArrayBuffer | undefined> {
    if (this.activeUpload) {
      // If this.activeUpload has been converted to int32, we need to convert it back to int64 data.
      const dstData = this.wrapper?.isInt64ToInt32Converted
        ? (convertInt32ToInt64(this.activeUpload) as Uint8Array)
        : this.activeUpload;

      if (dstBuffer) {
        if (dstBuffer instanceof ArrayBuffer) {
          new Uint8Array(dstBuffer).set(dstData);
        } else {
          new Uint8Array(dstBuffer.buffer, dstBuffer.byteOffset, dstBuffer.byteLength).set(dstData);
        }
        return;
      } else {
        return dstData.buffer;
      }
    }
    if (!this.wrapper) {
      throw new Error('Tensor has not been created.');
    }

    if (!dstBuffer) {
      return this.wrapper.read(this.wrapper?.shouldConvertInt64toInt32);
    }
    return this.wrapper.read(this.wrapper?.shouldConvertInt64toInt32, dstBuffer);
  }
}

class TensorManagerImpl implements TensorManager {
  private tensorTrackersById: Map<TensorId, TensorIdTracker> = new Map();
  private freeTensors: TensorWrapper[] = [];
  private externalTensors: Set<TensorWrapper> = new Set();

  constructor(private backend: WebNNBackend) {}

  public getMLContext(sessionId: number): MLContext {
    const context = this.backend.getMLContext(sessionId);
    if (!context) {
      throw new Error('MLContext not found for session.');
    }
    return context;
  }

  public reserveTensorId(): TensorId {
    const tensorId = createNewTensorId();
    this.tensorTrackersById.set(tensorId, new TensorIdTracker(this));
    return tensorId;
  }

  public releaseTensorId(tensorId: TensorId): void {
    const tensorTracker = this.tensorTrackersById.get(tensorId);
    if (!tensorTracker) {
      return;
    }
    this.tensorTrackersById.delete(tensorId);
    if (tensorTracker.tensorWrapper) {
      this.releaseTensor(tensorTracker.tensorWrapper);
    }
  }

  public async ensureTensor(
    sessionId: number,
    tensorId: TensorId,
    dataType: MLOperandDataType,
    shape: number[],
    copyOld: boolean,
  ): Promise<MLTensor> {
    LOG_DEBUG(
      'verbose',
      () =>
        `[WebNN] TensorManager.ensureTensor {tensorId: ${tensorId}, dataType: ${
          dataType
        }, shape: ${shape}, copyOld: ${copyOld}}`,
    );
    const tensor = this.tensorTrackersById.get(tensorId);
    if (!tensor) {
      throw new Error('Tensor not found.');
    }
    return tensor.ensureTensor(sessionId, dataType, shape, copyOld);
  }

  public upload(tensorId: TensorId, data: Uint8Array): void {
    const tensor = this.tensorTrackersById.get(tensorId);
    if (!tensor) {
      throw new Error('Tensor not found.');
    }
    tensor.upload(data);
  }

  public async download(tensorId: TensorId): Promise<ArrayBuffer>;
  public async download(tensorId: TensorId, dstBuffer: ArrayBufferView | ArrayBuffer): Promise<undefined>;
  async download(tensorId: TensorId, dstBuffer?: ArrayBufferView | ArrayBuffer): Promise<ArrayBuffer | undefined> {
    LOG_DEBUG(
      'verbose',
      () => `[WebNN] TensorManager.download {tensorId: ${tensorId}, dstBuffer: ${dstBuffer?.byteLength}}`,
    );
    const tensorTracker = this.tensorTrackersById.get(tensorId);
    if (!tensorTracker) {
      throw new Error('Tensor not found.');
    }
    return tensorTracker.download(dstBuffer);
  }

  public releaseTensorsForSession(sessionId: number): void {
    for (const tensor of this.freeTensors) {
      if (tensor.sessionId === sessionId) {
        tensor.destroy();
      }
    }
    this.freeTensors = this.freeTensors.filter((tensor) => tensor.sessionId !== sessionId);
  }

  public registerTensor(
    sessionId: number,
    mlTensor: MLTensor,
    dataType: MLOperandDataType,
    shape: readonly number[],
  ): TensorId {
    const context = this.getMLContext(sessionId);
    const tensorId = createNewTensorId();
    // Defaulting to READ | WRITE if usage is not provided.
    // eslint-disable-next-line no-bitwise
    const wrapper = new TensorWrapper({
      sessionId,
      context,
      tensor: mlTensor,
      dataType,
      shape,
    });
    this.tensorTrackersById.set(tensorId, new TensorIdTracker(this, wrapper));
    this.externalTensors.add(wrapper);
    return tensorId;
  }

  /**
   * Get or create an MLTensor with the given data type and shape.
   */
  public async getCachedTensor(
    sessionId: number,
    dataType: MLOperandDataType,
    shape: readonly number[],
    usage: MLTensorUsageFlags | undefined,
    writable: boolean,
    readable: boolean,
    shouldConvertInt64toInt32 = false,
  ): Promise<TensorWrapper> {
    const context = this.getMLContext(sessionId);
    for (const [index, tensor] of this.freeTensors.entries()) {
      if (tensor.canReuseTensor(context, dataType, shape)) {
        LOG_DEBUG('verbose', () => `[WebNN] Reusing tensor {dataType: ${dataType}, shape: ${shape}}`);
        const wrapper = this.freeTensors.splice(index, 1)[0];
        wrapper.sessionId = sessionId;
        return wrapper;
      }
    }
    LOG_DEBUG('verbose', () => `[WebNN] MLContext.createTensor {dataType: ${dataType}, shape: ${shape}}`);
    const tensor = await context.createTensor({
      dataType,
      shape,
      dimensions: shape,
      usage,
      writable,
      readable,
    });
    return new TensorWrapper({ sessionId, context, tensor, dataType, shape, shouldConvertInt64toInt32 });
  }

  /**
   * Release tensor for reuse unless external.
   */
  public releaseTensor(tensorWrapper: TensorWrapper) {
    if (this.externalTensors.has(tensorWrapper)) {
      this.externalTensors.delete(tensorWrapper);
    }
    this.freeTensors.push(tensorWrapper);
  }
}

export const createTensorManager = (...args: ConstructorParameters<typeof TensorManagerImpl>): TensorManager =>
  new TensorManagerImpl(...args);
