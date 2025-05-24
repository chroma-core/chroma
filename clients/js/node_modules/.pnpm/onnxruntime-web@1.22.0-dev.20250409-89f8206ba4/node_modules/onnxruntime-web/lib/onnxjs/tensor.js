'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
var __createBinding =
  (this && this.__createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);
        if (!desc || ('get' in desc ? !m.__esModule : desc.writable || desc.configurable)) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }
        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });
var __setModuleDefault =
  (this && this.__setModuleDefault) ||
  (Object.create
    ? function (o, v) {
        Object.defineProperty(o, 'default', { enumerable: true, value: v });
      }
    : function (o, v) {
        o['default'] = v;
      });
var __importStar =
  (this && this.__importStar) ||
  function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null)
      for (var k in mod)
        if (k !== 'default' && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
  };
var __importDefault =
  (this && this.__importDefault) ||
  function (mod) {
    return mod && mod.__esModule ? mod : { default: mod };
  };
Object.defineProperty(exports, '__esModule', { value: true });
exports.Tensor = void 0;
const guid_typescript_1 = require('guid-typescript');
const long_1 = __importDefault(require('long'));
const ortFbs = __importStar(require('./ort-schema/flatbuffers/ort-generated'));
const onnx_1 = require('./ort-schema/protobuf/onnx');
const util_1 = require('./util');
class Tensor {
  /**
   * get the underlying tensor data
   */
  get data() {
    if (this.cache === undefined) {
      const data = this.dataProvider(this.dataId);
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
    return this.data;
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
        return this.data;
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
        return this.data;
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
      return this.data;
    }
    throw new TypeError('type cannot be non-number (string)');
  }
  /**
   * get value of an element at the given indices
   */
  get(indices) {
    return this.data[util_1.ShapeUtil.indicesToOffset(indices, this.strides)];
  }
  /**
   * set value of an element at the given indices
   */
  set(indices, value) {
    this.data[util_1.ShapeUtil.indicesToOffset(indices, this.strides)] = value;
  }
  /**
   * get the underlying tensor data asynchronously
   */
  async getData() {
    if (this.cache === undefined) {
      this.cache = await this.asyncDataProvider(this.dataId);
    }
    return this.cache;
  }
  /**
   * get the strides for each dimension
   */
  get strides() {
    if (!this._strides) {
      this._strides = util_1.ShapeUtil.computeStrides(this.dims);
    }
    return this._strides;
  }
  constructor(
    /**
     * get the dimensions of the tensor
     */
    dims,
    /**
     * get the type of the tensor
     */
    type,
    dataProvider,
    asyncDataProvider,
    cache,
    /**
     * get the data ID that used to map to a tensor data
     */
    dataId = guid_typescript_1.Guid.create(),
  ) {
    this.dims = dims;
    this.type = type;
    this.dataProvider = dataProvider;
    this.asyncDataProvider = asyncDataProvider;
    this.cache = cache;
    this.dataId = dataId;
    this.size = util_1.ShapeUtil.validateDimsAndCalcSize(dims);
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
        this.cache = new Array(size);
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
  static fromProto(tensorProto) {
    if (!tensorProto) {
      throw new Error('cannot construct Value from an empty tensor');
    }
    const type = util_1.ProtoUtil.tensorDataTypeFromProto(tensorProto.dataType);
    const dims = util_1.ProtoUtil.tensorDimsFromProto(tensorProto.dims);
    const value = new Tensor(dims, type);
    if (type === 'string') {
      // When it's STRING type, the value should always be stored in field
      // 'stringData'
      tensorProto.stringData.forEach((str, i) => {
        value.data[i] = (0, util_1.decodeUtf8String)(str);
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
      const elementSize = sizeofProto(tensorProto.dataType);
      const length = tensorProto.rawData.byteLength / elementSize;
      if (tensorProto.rawData.byteLength % elementSize !== 0) {
        throw new Error('invalid buffer length');
      }
      if (dataDest.length !== length) {
        throw new Error('buffer length mismatch');
      }
      for (let i = 0; i < length; i++) {
        const n = readProto(dataSource, tensorProto.dataType, i * elementSize);
        dataDest[i] = n;
      }
    } else {
      // populate value from array
      let array;
      switch (tensorProto.dataType) {
        case onnx_1.onnx.TensorProto.DataType.FLOAT:
          array = tensorProto.floatData;
          break;
        case onnx_1.onnx.TensorProto.DataType.INT32:
        case onnx_1.onnx.TensorProto.DataType.INT16:
        case onnx_1.onnx.TensorProto.DataType.UINT16:
        case onnx_1.onnx.TensorProto.DataType.INT8:
        case onnx_1.onnx.TensorProto.DataType.UINT8:
        case onnx_1.onnx.TensorProto.DataType.BOOL:
          array = tensorProto.int32Data;
          break;
        case onnx_1.onnx.TensorProto.DataType.INT64:
          array = tensorProto.int64Data;
          break;
        case onnx_1.onnx.TensorProto.DataType.DOUBLE:
          array = tensorProto.doubleData;
          break;
        case onnx_1.onnx.TensorProto.DataType.UINT32:
        case onnx_1.onnx.TensorProto.DataType.UINT64:
          array = tensorProto.uint64Data;
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
        if (long_1.default.isLong(element)) {
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
  static fromData(data, dims, type) {
    return new Tensor(dims, type, undefined, undefined, data);
  }
  static fromOrtTensor(ortTensor) {
    if (!ortTensor) {
      throw new Error('cannot construct Value from an empty tensor');
    }
    const dims = util_1.ProtoUtil.tensorDimsFromORTFormat(ortTensor);
    const type = util_1.ProtoUtil.tensorDataTypeFromProto(ortTensor.dataType());
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
        ortTensor.rawDataArray().buffer,
        ortTensor.rawDataArray().byteOffset,
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
exports.Tensor = Tensor;
function sizeof(type) {
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
function sizeofProto(type) {
  switch (type) {
    case onnx_1.onnx.TensorProto.DataType.UINT8:
    case onnx_1.onnx.TensorProto.DataType.INT8:
    case onnx_1.onnx.TensorProto.DataType.BOOL:
      return 1;
    case onnx_1.onnx.TensorProto.DataType.UINT16:
    case onnx_1.onnx.TensorProto.DataType.INT16:
      return 2;
    case onnx_1.onnx.TensorProto.DataType.FLOAT:
    case onnx_1.onnx.TensorProto.DataType.INT32:
    case onnx_1.onnx.TensorProto.DataType.UINT32:
      return 4;
    case onnx_1.onnx.TensorProto.DataType.INT64:
    case onnx_1.onnx.TensorProto.DataType.DOUBLE:
    case onnx_1.onnx.TensorProto.DataType.UINT64:
      return 8;
    default:
      throw new Error(`cannot calculate sizeof() on type ${onnx_1.onnx.TensorProto.DataType[type]}`);
  }
}
function createView(dataBuffer, type) {
  return new (dataviewConstructor(type))(dataBuffer);
}
function dataviewConstructor(type) {
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
function longToNumber(i, type) {
  // INT64, UINT32, UINT64
  if (type === onnx_1.onnx.TensorProto.DataType.INT64 || type === ortFbs.TensorDataType.INT64) {
    if (i.greaterThanOrEqual(2147483648) || i.lessThan(-2147483648)) {
      throw new TypeError('int64 is not supported');
    }
  } else if (
    type === onnx_1.onnx.TensorProto.DataType.UINT32 ||
    type === ortFbs.TensorDataType.UINT32 ||
    type === onnx_1.onnx.TensorProto.DataType.UINT64 ||
    type === ortFbs.TensorDataType.UINT64
  ) {
    if (i.greaterThanOrEqual(4294967296) || i.lessThan(0)) {
      throw new TypeError('uint64 is not supported');
    }
  } else {
    throw new TypeError(`not a LONG type: ${onnx_1.onnx.TensorProto.DataType[type]}`);
  }
  return i.toNumber();
}
// read one value from TensorProto
function readProto(view, type, byteOffset) {
  switch (type) {
    case onnx_1.onnx.TensorProto.DataType.BOOL:
    case onnx_1.onnx.TensorProto.DataType.UINT8:
      return view.getUint8(byteOffset);
    case onnx_1.onnx.TensorProto.DataType.INT8:
      return view.getInt8(byteOffset);
    case onnx_1.onnx.TensorProto.DataType.UINT16:
      return view.getUint16(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.INT16:
      return view.getInt16(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.FLOAT:
      return view.getFloat32(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.INT32:
      return view.getInt32(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.UINT32:
      return view.getUint32(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.INT64:
      return longToNumber(
        long_1.default.fromBits(view.getUint32(byteOffset, true), view.getUint32(byteOffset + 4, true), false),
        type,
      );
    case onnx_1.onnx.TensorProto.DataType.DOUBLE:
      return view.getFloat64(byteOffset, true);
    case onnx_1.onnx.TensorProto.DataType.UINT64:
      return longToNumber(
        long_1.default.fromBits(view.getUint32(byteOffset, true), view.getUint32(byteOffset + 4, true), true),
        type,
      );
    default:
      throw new Error(`cannot read from DataView for type ${onnx_1.onnx.TensorProto.DataType[type]}`);
  }
}
//# sourceMappingURL=tensor.js.map
