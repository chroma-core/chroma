// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import Long from 'long';

import * as ortFbs from './ort-schema/flatbuffers/ort-generated';
import { onnx } from './ort-schema/protobuf/onnx';
import { Tensor } from './tensor';
import { decodeUtf8String, LongUtil } from './util';

export declare namespace Attribute {
  export interface DataTypeMap {
    float: number;
    int: number;
    string: string;
    tensor: Tensor;
    floats: number[];
    ints: number[];
    strings: string[];
    tensors: Tensor[];
  }

  export type DataType = keyof DataTypeMap;
}

type ValueTypes = Attribute.DataTypeMap[Attribute.DataType];

type Value = [ValueTypes, Attribute.DataType];

export class Attribute {
  constructor(attributes: onnx.IAttributeProto[] | ortFbs.Attribute[] | null | undefined) {
    this._attributes = new Map();
    if (attributes !== null && attributes !== undefined) {
      for (const attr of attributes) {
        if (attr instanceof onnx.AttributeProto) {
          this._attributes.set(attr.name, [Attribute.getValue(attr), Attribute.getType(attr)]);
        } else if (attr instanceof ortFbs.Attribute) {
          this._attributes.set(attr.name()!, [Attribute.getValue(attr), Attribute.getType(attr)]);
        }
      }
      if (this._attributes.size < attributes.length) {
        throw new Error('duplicated attribute names');
      }
    }
  }

  set(key: string, type: Attribute.DataType, value: ValueTypes): void {
    this._attributes.set(key, [value, type]);
  }
  delete(key: string): void {
    this._attributes.delete(key);
  }
  getFloat(key: string, defaultValue?: Attribute.DataTypeMap['float']) {
    return this.get(key, 'float', defaultValue);
  }

  getInt(key: string, defaultValue?: Attribute.DataTypeMap['int']) {
    return this.get(key, 'int', defaultValue);
  }

  getString(key: string, defaultValue?: Attribute.DataTypeMap['string']) {
    return this.get(key, 'string', defaultValue);
  }

  getTensor(key: string, defaultValue?: Attribute.DataTypeMap['tensor']) {
    return this.get(key, 'tensor', defaultValue);
  }

  getFloats(key: string, defaultValue?: Attribute.DataTypeMap['floats']) {
    return this.get(key, 'floats', defaultValue);
  }

  getInts(key: string, defaultValue?: Attribute.DataTypeMap['ints']) {
    return this.get(key, 'ints', defaultValue);
  }

  getStrings(key: string, defaultValue?: Attribute.DataTypeMap['strings']) {
    return this.get(key, 'strings', defaultValue);
  }

  getTensors(key: string, defaultValue?: Attribute.DataTypeMap['tensors']) {
    return this.get(key, 'tensors', defaultValue);
  }

  private get<V extends Attribute.DataTypeMap[Attribute.DataType]>(
    key: string,
    type: Attribute.DataType,
    defaultValue?: V,
  ): V {
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
    return valueAndType[0] as V;
  }

  private static getType(attr: onnx.IAttributeProto | ortFbs.Attribute): Attribute.DataType {
    const type = attr instanceof onnx.AttributeProto ? attr.type : (attr as ortFbs.Attribute).type();
    switch (type) {
      case onnx.AttributeProto.AttributeType.FLOAT:
        return 'float';
      case onnx.AttributeProto.AttributeType.INT:
        return 'int';
      case onnx.AttributeProto.AttributeType.STRING:
        return 'string';
      case onnx.AttributeProto.AttributeType.TENSOR:
        return 'tensor';
      case onnx.AttributeProto.AttributeType.FLOATS:
        return 'floats';
      case onnx.AttributeProto.AttributeType.INTS:
        return 'ints';
      case onnx.AttributeProto.AttributeType.STRINGS:
        return 'strings';
      case onnx.AttributeProto.AttributeType.TENSORS:
        return 'tensors';
      default:
        throw new Error(`attribute type is not supported yet: ${onnx.AttributeProto.AttributeType[type]}`);
    }
  }

  private static getValue(attr: onnx.IAttributeProto | ortFbs.Attribute) {
    const attrType = attr instanceof onnx.AttributeProto ? attr.type : (attr as ortFbs.Attribute).type();
    if (attrType === onnx.AttributeProto.AttributeType.GRAPH || attrType === onnx.AttributeProto.AttributeType.GRAPHS) {
      throw new Error('graph attribute is not supported yet');
    }

    const value = this.getValueNoCheck(attr);

    // cast LONG to number
    if (attrType === onnx.AttributeProto.AttributeType.INT && LongUtil.isLong(value)) {
      return LongUtil.longToNumber(value as bigint | Long);
    }

    // cast LONG[] to number[]
    if (attrType === onnx.AttributeProto.AttributeType.INTS) {
      const arr = value as Array<number | Long | bigint>;
      const numberValue: number[] = new Array<number>(arr.length);

      for (let i = 0; i < arr.length; i++) {
        const maybeLong = arr[i];
        numberValue[i] = LongUtil.longToNumber(maybeLong);
      }

      return numberValue;
    }

    // cast onnx.TensorProto to onnxjs.Tensor
    if (attrType === onnx.AttributeProto.AttributeType.TENSOR) {
      return attr instanceof onnx.AttributeProto
        ? Tensor.fromProto(value as onnx.ITensorProto)
        : Tensor.fromOrtTensor(value as ortFbs.Tensor);
    }

    // cast onnx.TensorProto[] to onnxjs.Tensor[]
    if (attrType === onnx.AttributeProto.AttributeType.TENSORS) {
      if (attr instanceof onnx.AttributeProto) {
        const tensorProtos = value as onnx.ITensorProto[];
        return tensorProtos.map((value) => Tensor.fromProto(value));
      } else if (attr instanceof ortFbs.Attribute) {
        const tensorProtos = value as ortFbs.Tensor[];
        return tensorProtos.map((value) => Tensor.fromOrtTensor(value));
      }
    }

    // cast Uint8Array to string
    if (attrType === onnx.AttributeProto.AttributeType.STRING) {
      // string in onnx attribute is of uint8array type, so we need to convert it to string below. While in ort format,
      // string attributes are returned as string, so no conversion is needed.
      if (attr instanceof onnx.AttributeProto) {
        const utf8String = value as Uint8Array;
        return decodeUtf8String(utf8String);
      }
    }

    // cast Uint8Array[] to string[]
    if (attrType === onnx.AttributeProto.AttributeType.STRINGS) {
      // strings in onnx attribute is returned as uint8array[], so we need to convert it to string[] below. While in ort
      // format strings attributes are returned as string[], so no conversion is needed.
      if (attr instanceof onnx.AttributeProto) {
        const utf8Strings = value as Uint8Array[];
        return utf8Strings.map(decodeUtf8String);
      }
    }

    return value as ValueTypes;
  }

  private static getValueNoCheck(attr: onnx.IAttributeProto | ortFbs.Attribute) {
    return attr instanceof onnx.AttributeProto
      ? this.getValueNoCheckFromOnnxFormat(attr)
      : this.getValueNoCheckFromOrtFormat(attr as ortFbs.Attribute);
  }

  private static getValueNoCheckFromOnnxFormat(attr: onnx.IAttributeProto) {
    switch (attr.type!) {
      case onnx.AttributeProto.AttributeType.FLOAT:
        return attr.f;
      case onnx.AttributeProto.AttributeType.INT:
        return attr.i;
      case onnx.AttributeProto.AttributeType.STRING:
        return attr.s;
      case onnx.AttributeProto.AttributeType.TENSOR:
        return attr.t;
      case onnx.AttributeProto.AttributeType.GRAPH:
        return attr.g;
      case onnx.AttributeProto.AttributeType.FLOATS:
        return attr.floats;
      case onnx.AttributeProto.AttributeType.INTS:
        return attr.ints;
      case onnx.AttributeProto.AttributeType.STRINGS:
        return attr.strings;
      case onnx.AttributeProto.AttributeType.TENSORS:
        return attr.tensors;
      case onnx.AttributeProto.AttributeType.GRAPHS:
        return attr.graphs;
      default:
        throw new Error(`unsupported attribute type: ${onnx.AttributeProto.AttributeType[attr.type!]}`);
    }
  }

  private static getValueNoCheckFromOrtFormat(attr: ortFbs.Attribute) {
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
          ints.push(attr.ints(i)!);
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
          tensors.push(attr.tensors(i)!);
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

  protected _attributes: Map<string, Value>;
}
