// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Guid } from 'guid-typescript';
import Long from 'long';

import * as ortFbs from './ort-schema/flatbuffers/ort-generated';
import { onnx } from './ort-schema/protobuf/onnx';
import { decodeUtf8String, ProtoUtil, ShapeUtil } from './util';

export declare namespace Tensor {
  export interface DataTypeMap {
    bool: Uint8Array;
    float32: Float32Array;
    float64: Float64Array;
    string: string[];
    int8: Int8Array;
    uint8: Uint8Array;
    int16: Int16Array;
    uint16: Uint16Array;
    int32: Int32Array;
    uint32: Uint32Array;
    int64: BigInt64Array;
  }

  export type DataType = keyof DataTypeMap;

  export type StringType = Tensor.DataTypeMap['string'];
  export type BooleanType = Tensor.DataTypeMap['bool'];
  export type IntegerType =
    | Tensor.DataTypeMap['int8']
    | Tensor.DataTypeMap['uint8']
    | Tensor.DataTypeMap['int16']
    | Tensor.DataTypeMap['uint16']
    | Tensor.DataTypeMap['int32']
    | Tensor.DataTypeMap['uint32'];
  export type FloatType = Tensor.DataTypeMap['float32'] | Tensor.DataTypeMap['float64'];
  export type NumberType = BooleanType | IntegerType | FloatType;

  export type Id = Guid;
}

type TensorData = Tensor.DataTypeMap[Tensor.DataType];

type DataProvider = (id: Tensor.Id) => TensorData;
type AsyncDataProvider = (id: Tensor.Id) => Promise<TensorData>;

export class Tensor {
  /**
   * get the underlying tensor data
   */
  get data(): TensorData {
    if (this.cache === undefined) {
      const data = this.dataProvider!(this.dataId);
      if (data.length !== this.size) {
        throw new Error('Length of data provided by the Data Provider is inconsistent with the dims of this Tensor.');
      }
      this.cache = data;
    }
    return this.cache;
  }

  /**
   * get the underlying string tensor data. Should only use when type is STRING
   */
  get stringData() {
    if (this.type !== 'string') {
      throw new TypeError('data type is not string');
    }

    return this.data as Tensor.StringType;
  }

  /**
   * get the underlying integer tensor data. Should only use when type is one of the following: (UINT8, INT8, UINT16,
   * INT16, INT32, UINT32, BOOL)
   */
  get integerData() {
    switch (this.type) {
      case 'uint8':
      case 'int8':
      case 'uint16':
      case 'int16':
      case 'int32':
      case 'uint32':
      case 'bool':
        return this.data as Tensor.IntegerType;

      default:
        throw new TypeError('data type is not integer (uint8, int8, uint16, int16, int32, uint32, bool)');
    }
  }

  /**
   * get the underlying float tensor data. Should only use when type is one of the following: (FLOAT, DOUBLE)
   */
  get floatData() {
    switch (this.type) {
      case 'float32':
      case 'float64':
        return this.data as Tensor.FloatType;

      default:
        throw new TypeError('data type is not float (float32, float64)');
    }
  }

  /**
   * get the underlying number tensor data. Should only use when type is one of the following: (UINT8, INT8, UINT16,
   * INT16, INT32, UINT32, BOOL, FLOAT, DOUBLE)
   */
  get numberData() {
    if (this.type !== 'string') {
      return this.data as Tensor.NumberType;
    }
    throw new TypeError('type cannot be non-number (string)');
  }

  /**
   * get value of an element at the given indices
   */
  get(indices: readonly number[]): Tensor.DataTypeMap[Tensor.DataType][number] {
    return this.data[ShapeUtil.indicesToOffset(indices, this.strides)];
  }

  /**
   * set value of an element at the given indices
   */
  set(indices: readonly number[], value: Tensor.DataTypeMap[Tensor.DataType][number]) {
    this.data[ShapeUtil.indicesToOffset(indices, this.strides)] = value;
  }

  /**
   * get the underlying tensor data asynchronously
   */
  async getData(): Promise<TensorData> {
    if (this.cache === undefined) {
      this.cache = await this.asyncDataProvider!(this.dataId);
    }
    return this.cache;
  }

  /**
   * get the number of elements in the tensor
   */
  public readonly size: number;

  private _strides: readonly number[];
  /**
   * get the strides for each dimension
   */
  get strides(): readonly number[] {
    if (!this._strides) {
      this._strides = ShapeUtil.computeStrides(this.dims);
    }
    return this._strides;
  }

  constructor(
    /**
     * get the dimensions of the tensor
     */
    public readonly dims: readonly number[],
    /**
     * get the type of the tensor
     */
    public readonly type: Tensor.DataType,
    private dataProvider?: DataProvider,
    private asyncDataProvider?: AsyncDataProvider,
    private cache?: TensorData,
    /**
     * get the data ID that used to map to a tensor data
     */
    public readonly dataId: Guid = Guid.create(),
  ) {
    this.size = ShapeUtil.validateDimsAndCalcSize(dims);
    const size = this.size;
    const empty = dataProvider === undefined && asyncDataProvider === undefined && cache === undefined;

    if (cache !== undefined) {
      if (cache.length !== size) {
        throw new RangeError("Input dims doesn't match data length.");
      }
    }

    if (type === 'string') {
      if (cache !== undefined && (!Array.isArray(cache) || !cache.every((i) => typeof i === 'string'))) {
        throw new TypeError('cache should be a string array');
      }

      if (empty) {
        this.cache = new Array<string>(size);
      }
    } else {
      if (cache !== undefined) {
        const constructor = dataviewConstructor(type);
        if (!(cache instanceof constructor)) {
          throw new TypeError(`cache should be type ${constructor.name}`);
        }
      }

      if (empty) {
        const buf = new ArrayBuffer(size * sizeof(type));
        this.cache = createView(buf, type);
      }
    }
  }

  /**
   * Construct new Tensor from a ONNX Tensor object
   * @param tensorProto the ONNX Tensor
   */
  static fromProto(tensorProto: onnx.ITensorProto): Tensor {
    if (!tensorProto) {
      throw new Error('cannot construct Value from an empty tensor');
    }
    const type = ProtoUtil.tensorDataTypeFromProto(tensorProto.dataType!);
    const dims = ProtoUtil.tensorDimsFromProto(tensorProto.dims!);

    const value = new Tensor(dims, type);

    if (type === 'string') {
      // When it's STRING type, the value should always be stored in field
      // 'stringData'
      tensorProto.stringData!.forEach((str, i) => {
        value.data[i] = decodeUtf8String(str);
      });
    } else if (
      tensorProto.rawData &&
      typeof tensorProto.rawData.byteLength === 'number' &&
      tensorProto.rawData.byteLength > 0
    ) {
      // NOT considering segment for now (IMPORTANT)

      // populate value from rawData
      const dataDest = value.data;
      const dataSource = new DataView(
        tensorProto.rawData.buffer,
        tensorProto.rawData.byteOffset,
        tensorProto.rawData.byteLength,
      );
      const elementSize = sizeofProto(tensorProto.dataType!);
      const length = tensorProto.rawData.byteLength / elementSize;

      if (tensorProto.rawData.byteLength % elementSize !== 0) {
        throw new Error('invalid buffer length');
      }
      if (dataDest.length !== length) {
        throw new Error('buffer length mismatch');
      }

      for (let i = 0; i < length; i++) {
        const n = readProto(dataSource, tensorProto.dataType!, i * elementSize);
        dataDest[i] = n;
      }
    } else {
      // populate value from array
      let array: Array<number | Long>;
      switch (tensorProto.dataType) {
        case onnx.TensorProto.DataType.FLOAT:
          array = tensorProto.floatData!;
          break;
        case onnx.TensorProto.DataType.INT32:
        case onnx.TensorProto.DataType.INT16:
        case onnx.TensorProto.DataType.UINT16:
        case onnx.TensorProto.DataType.INT8:
        case onnx.TensorProto.DataType.UINT8:
        case onnx.TensorProto.DataType.BOOL:
          array = tensorProto.int32Data!;
          break;
        case onnx.TensorProto.DataType.INT64:
          array = tensorProto.int64Data!;
          break;
        case onnx.TensorProto.DataType.DOUBLE:
          array = tensorProto.doubleData!;
          break;
        case onnx.TensorProto.DataType.UINT32:
        case onnx.TensorProto.DataType.UINT64:
          array = tensorProto.uint64Data!;
          break;
        default:
          // should never run here
          throw new Error('unspecific error');
      }

      if (array === null || array === undefined) {
        throw new Error('failed to populate data from a tensorproto value');
      }

      const data = value.data;
      if (data.length !== array.length) {
        throw new Error('array length mismatch');
      }

      for (let i = 0; i < array.length; i++) {
        const element = array[i];
        if (Long.isLong(element)) {
          data[i] = longToNumber(element, tensorProto.dataType);
        } else {
          data[i] = element;
        }
      }
    }

    return value;
  }

  /**
   * Construct new Tensor from raw data
   * @param data the raw data object. Should be a string array for 'string' tensor, and the corresponding typed array
   * for other types of tensor.
   * @param dims the dimensions of the tensor
   * @param type the type of the tensor
   */
  static fromData(data: Tensor.DataTypeMap[Tensor.DataType], dims: readonly number[], type: Tensor.DataType) {
    return new Tensor(dims, type, undefined, undefined, data);
  }

  static fromOrtTensor(ortTensor: ortFbs.Tensor) {
    if (!ortTensor) {
      throw new Error('cannot construct Value from an empty tensor');
    }
    const dims = ProtoUtil.tensorDimsFromORTFormat(ortTensor);
    const type = ProtoUtil.tensorDataTypeFromProto(ortTensor.dataType());

    const value = new Tensor(dims, type);

    if (type === 'string') {
      // When it's STRING type, the value should always be stored in field
      // 'stringData'
      for (let i = 0; i < ortTensor.stringDataLength(); i++) {
        value.data[i] = ortTensor.stringData(i);
      }
    } else if (
      ortTensor.rawDataArray() &&
      typeof ortTensor.rawDataLength() === 'number' &&
      ortTensor.rawDataLength() > 0
    ) {
      // NOT considering segment for now (IMPORTANT)

      // populate value from rawData
      const dataDest = value.data;
      const dataSource = new DataView(
        ortTensor.rawDataArray()!.buffer,
        ortTensor.rawDataArray()!.byteOffset,
        ortTensor.rawDataLength(),
      );
      const elementSize = sizeofProto(ortTensor.dataType());
      const length = ortTensor.rawDataLength() / elementSize;

      if (ortTensor.rawDataLength() % elementSize !== 0) {
        throw new Error('invalid buffer length');
      }
      if (dataDest.length !== length) {
        throw new Error('buffer length mismatch');
      }

      for (let i = 0; i < length; i++) {
        const n = readProto(dataSource, ortTensor.dataType(), i * elementSize);
        dataDest[i] = n;
      }
    }
    return value;
  }
}

function sizeof(type: Tensor.DataType): number {
  switch (type) {
    case 'bool':
    case 'int8':
    case 'uint8':
      return 1;
    case 'int16':
    case 'uint16':
      return 2;
    case 'int32':
    case 'uint32':
    case 'float32':
      return 4;
    case 'float64':
      return 8;
    default:
      throw new Error(`cannot calculate sizeof() on type ${type}`);
  }
}

function sizeofProto(type: onnx.TensorProto.DataType | ortFbs.TensorDataType): number {
  switch (type) {
    case onnx.TensorProto.DataType.UINT8:
    case onnx.TensorProto.DataType.INT8:
    case onnx.TensorProto.DataType.BOOL:
      return 1;
    case onnx.TensorProto.DataType.UINT16:
    case onnx.TensorProto.DataType.INT16:
      return 2;
    case onnx.TensorProto.DataType.FLOAT:
    case onnx.TensorProto.DataType.INT32:
    case onnx.TensorProto.DataType.UINT32:
      return 4;
    case onnx.TensorProto.DataType.INT64:
    case onnx.TensorProto.DataType.DOUBLE:
    case onnx.TensorProto.DataType.UINT64:
      return 8;
    default:
      throw new Error(`cannot calculate sizeof() on type ${onnx.TensorProto.DataType[type]}`);
  }
}

function createView(dataBuffer: ArrayBuffer, type: Tensor.DataType) {
  return new (dataviewConstructor(type))(dataBuffer);
}

function dataviewConstructor(type: Tensor.DataType) {
  switch (type) {
    case 'bool':
    case 'uint8':
      return Uint8Array;
    case 'int8':
      return Int8Array;
    case 'int16':
      return Int16Array;
    case 'uint16':
      return Uint16Array;
    case 'int32':
      return Int32Array;
    case 'uint32':
      return Uint32Array;
    case 'int64':
      return BigInt64Array;
    case 'float32':
      return Float32Array;
    case 'float64':
      return Float64Array;
    default:
      // should never run to here
      throw new Error('unspecified error');
  }
}

// convert a long number to a 32-bit integer (cast-down)
function longToNumber(i: Long, type: onnx.TensorProto.DataType | ortFbs.TensorDataType): number {
  // INT64, UINT32, UINT64
  if (type === onnx.TensorProto.DataType.INT64 || type === ortFbs.TensorDataType.INT64) {
    if (i.greaterThanOrEqual(2147483648) || i.lessThan(-2147483648)) {
      throw new TypeError('int64 is not supported');
    }
  } else if (
    type === onnx.TensorProto.DataType.UINT32 ||
    type === ortFbs.TensorDataType.UINT32 ||
    type === onnx.TensorProto.DataType.UINT64 ||
    type === ortFbs.TensorDataType.UINT64
  ) {
    if (i.greaterThanOrEqual(4294967296) || i.lessThan(0)) {
      throw new TypeError('uint64 is not supported');
    }
  } else {
    throw new TypeError(`not a LONG type: ${onnx.TensorProto.DataType[type]}`);
  }

  return i.toNumber();
}

// read one value from TensorProto
function readProto(
  view: DataView,
  type: onnx.TensorProto.DataType | ortFbs.TensorDataType,
  byteOffset: number,
): number {
  switch (type) {
    case onnx.TensorProto.DataType.BOOL:
    case onnx.TensorProto.DataType.UINT8:
      return view.getUint8(byteOffset);
    case onnx.TensorProto.DataType.INT8:
      return view.getInt8(byteOffset);
    case onnx.TensorProto.DataType.UINT16:
      return view.getUint16(byteOffset, true);
    case onnx.TensorProto.DataType.INT16:
      return view.getInt16(byteOffset, true);
    case onnx.TensorProto.DataType.FLOAT:
      return view.getFloat32(byteOffset, true);
    case onnx.TensorProto.DataType.INT32:
      return view.getInt32(byteOffset, true);
    case onnx.TensorProto.DataType.UINT32:
      return view.getUint32(byteOffset, true);
    case onnx.TensorProto.DataType.INT64:
      return longToNumber(
        Long.fromBits(view.getUint32(byteOffset, true), view.getUint32(byteOffset + 4, true), false),
        type,
      );
    case onnx.TensorProto.DataType.DOUBLE:
      return view.getFloat64(byteOffset, true);
    case onnx.TensorProto.DataType.UINT64:
      return longToNumber(
        Long.fromBits(view.getUint32(byteOffset, true), view.getUint32(byteOffset + 4, true), true),
        type,
      );
    default:
      throw new Error(`cannot read from DataView for type ${onnx.TensorProto.DataType[type]}`);
  }
}
