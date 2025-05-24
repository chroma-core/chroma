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
Object.defineProperty(exports, '__esModule', { value: true });
exports.Attribute = void 0;
const ortFbs = __importStar(require('./ort-schema/flatbuffers/ort-generated'));
const onnx_1 = require('./ort-schema/protobuf/onnx');
const tensor_1 = require('./tensor');
const util_1 = require('./util');
class Attribute {
  constructor(attributes) {
    this._attributes = new Map();
    if (attributes !== null && attributes !== undefined) {
      for (const attr of attributes) {
        if (attr instanceof onnx_1.onnx.AttributeProto) {
          this._attributes.set(attr.name, [Attribute.getValue(attr), Attribute.getType(attr)]);
        } else if (attr instanceof ortFbs.Attribute) {
          this._attributes.set(attr.name(), [Attribute.getValue(attr), Attribute.getType(attr)]);
        }
      }
      if (this._attributes.size < attributes.length) {
        throw new Error('duplicated attribute names');
      }
    }
  }
  set(key, type, value) {
    this._attributes.set(key, [value, type]);
  }
  delete(key) {
    this._attributes.delete(key);
  }
  getFloat(key, defaultValue) {
    return this.get(key, 'float', defaultValue);
  }
  getInt(key, defaultValue) {
    return this.get(key, 'int', defaultValue);
  }
  getString(key, defaultValue) {
    return this.get(key, 'string', defaultValue);
  }
  getTensor(key, defaultValue) {
    return this.get(key, 'tensor', defaultValue);
  }
  getFloats(key, defaultValue) {
    return this.get(key, 'floats', defaultValue);
  }
  getInts(key, defaultValue) {
    return this.get(key, 'ints', defaultValue);
  }
  getStrings(key, defaultValue) {
    return this.get(key, 'strings', defaultValue);
  }
  getTensors(key, defaultValue) {
    return this.get(key, 'tensors', defaultValue);
  }
  get(key, type, defaultValue) {
    const valueAndType = this._attributes.get(key);
    if (valueAndType === undefined) {
      if (defaultValue !== undefined) {
        return defaultValue;
      }
      throw new Error(`required attribute not found: ${key}`);
    }
    if (valueAndType[1] !== type) {
      throw new Error(`type mismatch: expected ${type} but got ${valueAndType[1]}`);
    }
    return valueAndType[0];
  }
  static getType(attr) {
    const type = attr instanceof onnx_1.onnx.AttributeProto ? attr.type : attr.type();
    switch (type) {
      case onnx_1.onnx.AttributeProto.AttributeType.FLOAT:
        return 'float';
      case onnx_1.onnx.AttributeProto.AttributeType.INT:
        return 'int';
      case onnx_1.onnx.AttributeProto.AttributeType.STRING:
        return 'string';
      case onnx_1.onnx.AttributeProto.AttributeType.TENSOR:
        return 'tensor';
      case onnx_1.onnx.AttributeProto.AttributeType.FLOATS:
        return 'floats';
      case onnx_1.onnx.AttributeProto.AttributeType.INTS:
        return 'ints';
      case onnx_1.onnx.AttributeProto.AttributeType.STRINGS:
        return 'strings';
      case onnx_1.onnx.AttributeProto.AttributeType.TENSORS:
        return 'tensors';
      default:
        throw new Error(`attribute type is not supported yet: ${onnx_1.onnx.AttributeProto.AttributeType[type]}`);
    }
  }
  static getValue(attr) {
    const attrType = attr instanceof onnx_1.onnx.AttributeProto ? attr.type : attr.type();
    if (
      attrType === onnx_1.onnx.AttributeProto.AttributeType.GRAPH ||
      attrType === onnx_1.onnx.AttributeProto.AttributeType.GRAPHS
    ) {
      throw new Error('graph attribute is not supported yet');
    }
    const value = this.getValueNoCheck(attr);
    // cast LONG to number
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.INT && util_1.LongUtil.isLong(value)) {
      return util_1.LongUtil.longToNumber(value);
    }
    // cast LONG[] to number[]
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.INTS) {
      const arr = value;
      const numberValue = new Array(arr.length);
      for (let i = 0; i < arr.length; i++) {
        const maybeLong = arr[i];
        numberValue[i] = util_1.LongUtil.longToNumber(maybeLong);
      }
      return numberValue;
    }
    // cast onnx.TensorProto to onnxjs.Tensor
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.TENSOR) {
      return attr instanceof onnx_1.onnx.AttributeProto
        ? tensor_1.Tensor.fromProto(value)
        : tensor_1.Tensor.fromOrtTensor(value);
    }
    // cast onnx.TensorProto[] to onnxjs.Tensor[]
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.TENSORS) {
      if (attr instanceof onnx_1.onnx.AttributeProto) {
        const tensorProtos = value;
        return tensorProtos.map((value) => tensor_1.Tensor.fromProto(value));
      } else if (attr instanceof ortFbs.Attribute) {
        const tensorProtos = value;
        return tensorProtos.map((value) => tensor_1.Tensor.fromOrtTensor(value));
      }
    }
    // cast Uint8Array to string
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.STRING) {
      // string in onnx attribute is of uint8array type, so we need to convert it to string below. While in ort format,
      // string attributes are returned as string, so no conversion is needed.
      if (attr instanceof onnx_1.onnx.AttributeProto) {
        const utf8String = value;
        return (0, util_1.decodeUtf8String)(utf8String);
      }
    }
    // cast Uint8Array[] to string[]
    if (attrType === onnx_1.onnx.AttributeProto.AttributeType.STRINGS) {
      // strings in onnx attribute is returned as uint8array[], so we need to convert it to string[] below. While in ort
      // format strings attributes are returned as string[], so no conversion is needed.
      if (attr instanceof onnx_1.onnx.AttributeProto) {
        const utf8Strings = value;
        return utf8Strings.map(util_1.decodeUtf8String);
      }
    }
    return value;
  }
  static getValueNoCheck(attr) {
    return attr instanceof onnx_1.onnx.AttributeProto
      ? this.getValueNoCheckFromOnnxFormat(attr)
      : this.getValueNoCheckFromOrtFormat(attr);
  }
  static getValueNoCheckFromOnnxFormat(attr) {
    switch (attr.type) {
      case onnx_1.onnx.AttributeProto.AttributeType.FLOAT:
        return attr.f;
      case onnx_1.onnx.AttributeProto.AttributeType.INT:
        return attr.i;
      case onnx_1.onnx.AttributeProto.AttributeType.STRING:
        return attr.s;
      case onnx_1.onnx.AttributeProto.AttributeType.TENSOR:
        return attr.t;
      case onnx_1.onnx.AttributeProto.AttributeType.GRAPH:
        return attr.g;
      case onnx_1.onnx.AttributeProto.AttributeType.FLOATS:
        return attr.floats;
      case onnx_1.onnx.AttributeProto.AttributeType.INTS:
        return attr.ints;
      case onnx_1.onnx.AttributeProto.AttributeType.STRINGS:
        return attr.strings;
      case onnx_1.onnx.AttributeProto.AttributeType.TENSORS:
        return attr.tensors;
      case onnx_1.onnx.AttributeProto.AttributeType.GRAPHS:
        return attr.graphs;
      default:
        throw new Error(`unsupported attribute type: ${onnx_1.onnx.AttributeProto.AttributeType[attr.type]}`);
    }
  }
  static getValueNoCheckFromOrtFormat(attr) {
    switch (attr.type()) {
      case ortFbs.AttributeType.FLOAT:
        return attr.f();
      case ortFbs.AttributeType.INT:
        return attr.i();
      case ortFbs.AttributeType.STRING:
        return attr.s();
      case ortFbs.AttributeType.TENSOR:
        return attr.t();
      case ortFbs.AttributeType.GRAPH:
        return attr.g();
      case ortFbs.AttributeType.FLOATS:
        return attr.floatsArray();
      case ortFbs.AttributeType.INTS: {
        const ints = [];
        for (let i = 0; i < attr.intsLength(); i++) {
          ints.push(attr.ints(i));
        }
        return ints;
      }
      case ortFbs.AttributeType.STRINGS: {
        const strings = [];
        for (let i = 0; i < attr.stringsLength(); i++) {
          strings.push(attr.strings(i));
        }
        return strings;
      }
      case ortFbs.AttributeType.TENSORS: {
        const tensors = [];
        for (let i = 0; i < attr.tensorsLength(); i++) {
          tensors.push(attr.tensors(i));
        }
        return tensors;
      }
      // case ortFbs.AttributeType.GRAPHS:
      // TODO: Subgraph not supported yet.
      // const graphs = [];
      // for (let i = 0; i < attr.graphsLength(); i++) {
      //   graphs.push(attr.graphs(i)!);
      // }
      // return graphs;
      default:
        throw new Error(`unsupported attribute type: ${ortFbs.AttributeType[attr.type()]}`);
    }
  }
}
exports.Attribute = Attribute;
//# sourceMappingURL=attribute.js.map
