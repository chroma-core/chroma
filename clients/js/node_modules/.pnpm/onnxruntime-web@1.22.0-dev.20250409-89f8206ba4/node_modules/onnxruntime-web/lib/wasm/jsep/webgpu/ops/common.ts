// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { ShapeUtil } from '../../util';
import { ProgramUniform, ProgramUniformVariableInfo } from '../types';

/**
 * constant value for a workgroup size.
 *
 * We definitely can do further optimization in future, but for now we use 64.
 *
 * rule of thumb: Use [a workgroup size of] 64 unless you know what GPU you are targeting or that your workload
 *                needs something different.
 *
 * from: https://surma.dev/things/webgpu/
 **/
export const WORKGROUP_SIZE = 64;

interface IndicesHelperTypes {
  /**
   * WGSL type of indices expression
   */
  readonly indices: string;

  /**
   * WGSL type of a value
   */
  readonly value: string;

  /**
   * WGSL type of storage type representing a value
   *
   * This is usually the same to `value`, but for some type (eg. bool), we need to use `u32` as storage type for
   * value type `vec4<bool>`
   */
  readonly storage: string;

  /**
   * tensor type as represented in TensorView
   */
  readonly tensor: number;
}

/**
 * A helper class for generating WGSL code for manipulating indices and data for a shader's input or output.
 *
 * This class is designed to offer a unified way to generate WGSL code for manipulating indices and data for a shader's
 * input or output.
 *
 * The following is a list of terminologies used in this class:
 * - `offset`: a uint32 value representing the offset of an element in the data buffer.
 * - `indices`: an abstraction of a multi-dimensional array's indices representing the data's index on each dimension.
 * - `value`: a value of a data element.
 *
 * Users are expected to create an instance of this class for each shader's input or output, and use the instance to
 * generate WGSL code for manipulating indices and data. The following 2 exported functions are for users to call to
 * create an instance of an indices helper:
 * - `inputVariable()`: create an indices helper instance for an input.
 * - `outputVariable()`: create an indices helper instance for an output.
 * - `internalVariable()`: create an indices helper instance for an internal variable.
 *
 * An indices helper instance contains helper functions for the following operations:
 * - access readonly basic information, including: `name`(the name of the input or output), `usage`(whether it's an
 * input, an output or an internal variable) and `shape`(the passed in shape).
 * - `type`: access readonly type information, including: `indices`(the type of indices), `value`(the type of value at
 * runtime), `storage`(the type of value at storage) and `tensor`(the tensor type as represented in TensorView).
 * - generate WGSL code for getting indices from offset. Use `offsetToIndices()` for WGSL code snippet to calculate
 * indices from offset, and use `indicesToOffset()` for WGSL code snippet to calculate offset from indices.
 * - to manipulate an instance of indices, use `setIndices()` and `getIndices()` to set and get the indices on an
 * indices variable.
 * - to manipulate data, use `set()`/`get()` to access data at the given indices from parameter list, use
 * `setByIndices()`/`getByIndices()` to access data at the given indices from an indices variable, and use
 * `setByOffset()`/`getByOffset()` to access data at the given offset.
 * - `impl`: get WGSL code of function implementation for the util functions mentioned above.
 */
export interface IndicesHelper {
  /**
   * get WGSL code of function implementation for the util functions.
   *
   */
  readonly impl: () => string;

  /**
   * get type info
   */
  readonly type: IndicesHelperTypes;

  /**
   * WGSL code of a expression for getting indices from offset.
   *
   * @param varOffset - a u32 expression representing the offset.
   *
   * @returns an `type.indices` expression
   */
  readonly offsetToIndices: (varOffset: string) => string;

  /**
   * WGSL code of an `u32` expression for getting offset from indices.
   *
   * @param varIndices - a `type.indices` expression representing the indices.
   *
   * @returns an `u32` expression
   */
  readonly indicesToOffset: (varIndices: string) => string;

  /**
   * WGSL code of an `u32` expression for getting original offset from broadcasted indices.
   *
   * @param varIndices - a `type.indices` expression representing the output indices.
   * @param output - output IndicesHelper.
   *
   * @returns an `u32` expression
   */
  readonly broadcastedIndicesToOffset: (varIndices: string, output: IndicesHelper) => string;

  /**
   * WGSL code of generating an indices literal
   *
   * @param init - initial value.
   */
  readonly indices: (...init: ReadonlyArray<number | string>) => string;

  /**
   * WGSL code of a statement for setting indices.
   *
   * @param varIndices - a variable name for the indices.
   * @param idx - the index of the indices to set. can be a number or a string (WGSL `u32` expression).
   * @param value - the value to set. can be a number or a string (WGSL `u32` expression).
   *
   * @returns a WGSL statement
   */
  readonly indicesSet: (varIndices: string, idx: number | string, value: number | string) => void;

  /**
   * WGSL code of an `u32` expression for getting indices.
   *
   * @param varIndices - a variable name for the indices.
   * @param idx - the index of the indices to get. can be a number or a string (WGSL `u32` expression).
   *
   * @returns an `u32` expression
   */
  readonly indicesGet: (varIndices: string, idx: number | string) => string;

  /**
   * WGSL code for a statement for setting data at the given indices.
   *
   * @param indicesAndValue - an array of numbers or strings (WGSL `u32` expression) representing the indices, followed
   *     by the value to set. This array should have exactly `shape.length + 1` elements.
   */
  readonly set: (...indicesAndValue: ReadonlyArray<number | string>) => string;

  /**
   * WGSL code for a statement for setting data at the given indices variable.
   *
   * @param varIndices - a variable name for the indices.
   * @param value - the value to set. should be a WGSL expression.
   */
  readonly setByIndices: (varIndices: string, value: string) => string;

  /**
   * WGSL code for a statement for setting data at the given offset.
   *
   * @param offset - a number or a string (WGSL `u32` expression) representing the offset.
   * @param value - the value to set. should be a WGSL expression.
   */
  readonly setByOffset: (offset: number | string, value: string) => string;

  /**
   * WGSL code for an expression for getting data at the given indices.
   *
   * @param indices - an array of numbers or strings (WGSL `u32` expression) representing the indices.
   */
  readonly get: (...indices: ReadonlyArray<number | string>) => string;

  /**
   * WGSL code for an expression for getting data at the given indices variable.
   *
   * @param varIndices - a variable name for the indices.
   */
  readonly getByIndices: (varIndices: string) => string;

  /**
   * WGSL code for an expression for getting data at the given offset.
   *
   * @param offset - a number or a string (WGSL `u32` expression) representing the offset.
   */
  readonly getByOffset: (offset: number | string) => string;

  /**
   * name of the data variable
   */
  readonly name: string;

  /**
   * whether the helper is for an input, an output or an internal variable.
   */
  readonly usage: 'input' | 'output' | 'atomicOutput' | 'internal';

  /**
   * the rank of the input or output.
   */
  readonly rank: number;

  /**
   * a string representing the variable name for the shape of the input or output.
   */
  readonly shape: string;

  /**
   * a string representing the variable name for the strides of the input or output.
   */
  readonly strides: string;
}

const getWgslMappedType = (type: number, components: 1 | 2 | 3 | 4): string | [string, string] => {
  if (components === 3) {
    throw new Error('vec3 has same alignment as vec4, use vec4 instead');
  }

  // return type is [ storage type, runtime type ] or a single string for both
  switch (Number(type)) {
    case DataType.float16:
      return components > 1 ? `vec${components}<f16>` : 'f16';
    case DataType.float:
      return components > 1 ? `vec${components}<f32>` : 'f32';
    case DataType.int32:
      return components > 1 ? `vec${components}<i32>` : 'i32';
    case DataType.uint32:
      return components > 1 ? `vec${components}<u32>` : 'u32';
    case DataType.int64:
      if (components > 1) {
        throw new Error('currently not supported vecX of uint64 yet');
      }
      return ['vec2<u32>', 'i32'];
    case DataType.uint64:
      if (components > 1) {
        throw new Error('currently not supported vecX of uint64 yet');
      }
      return ['vec2<u32>', 'u32'];
    case DataType.bool:
      if (components !== 4) {
        throw new Error('bool must be vec4');
      }
      return ['u32', 'vec4<bool>'];
    case DataType.int4:
      return 'i32';
    case DataType.uint4:
      return 'u32';
    default:
      throw new Error(`Unknown data type: ${type}`);
  }
};

export const tensorTypeToWsglStorageType = (type: DataType, components: 1 | 2 | 3 | 4 = 1) => {
  const mappedType = getWgslMappedType(type, components);
  return typeof mappedType === 'string' ? mappedType : mappedType[0];
};

export const tensorTypeToWsglValueType = (type: DataType, components: 1 | 2 | 3 | 4 = 1) => {
  const mappedType = getWgslMappedType(type, components);
  return typeof mappedType === 'string' ? mappedType : mappedType[1];
};

export const createTensorShapeVariables = (...dims: ReadonlyArray<readonly number[]>): ProgramUniform[] => {
  const programUniforms: ProgramUniform[] = [];
  dims.forEach((dim) => {
    if (dim.length !== 0) {
      programUniforms.push(
        { type: DataType.uint32, data: dim },
        { type: DataType.uint32, data: ShapeUtil.computeStrides(dim) },
      );
    }
  });
  return programUniforms;
};

/**
 * A helper function to get maximum vector size for specified data length
 * @param size
 */
export const getMaxComponents = (size: number) => {
  // we cannot use vec3 type since it has alignment of 16 bytes
  if (size % 4 === 0) {
    return 4;
  } else if (size % 2 === 0) {
    return 2;
  }

  return 1;
};

/**
 * A helper function that initializes variable as a scalar or vector. e.g. f32(0) or vec4f(0,0,0,0)
 * @param dataType
 * @param components
 * @param value
 */
export const fillVector = (dataType = 'f32', components?: number, value = '0') => {
  if (!components || components === 1) {
    return `${dataType}(${value})`;
  }

  return `vec${components}<${dataType}>(${value})`;
};

/**
 * A helper function that casts value or vector to f32
 * @param dataType
 * @param components
 * @param value
 */
export const castToF32 = (dataType: string, components: number, value: string) => {
  if (dataType === 'f32') {
    return value;
  }
  if (components === 1) {
    return `f32(${value})`;
  }

  return `vec${components}<f32>(${value})`;
};

/**
 * A helper function that returns scalar or sums all components of a vector
 * @param name
 * @param components
 */
export const sumVector = (name: string, components: number) => {
  if (components === 4) {
    return `(${name}.x + ${name}.y + ${name}.z + ${name}.w)`;
  } else if (components === 2) {
    return `(${name}.x + ${name}.y)`;
  } else if (components === 3) {
    return `(${name}.x + ${name}.y + ${name}.z)`;
  }

  return name;
};

/**
 * A helper function that returns variable element at index.
 * @param name - the name of variable.
 * @param index - the index of variable element.
 * @param length - the length of variable.
 * @param type - the type of variable, optional.
 */
export const getElementAt = (
  name: string,
  index: number | string,
  length: number,
  type?: UniformDataElementType,
): string => {
  if (name.startsWith('uniforms.') && length > 4) {
    if (typeof index === 'string') {
      if (type === 'f16') {
        return `${name}[(${index}) / 8][(${index}) % 8 / 4][(${index}) % 8 % 4]`;
      } else {
        return `${name}[(${index}) / 4][(${index}) % 4]`;
      }
    } else {
      if (type === 'f16') {
        return `${name}[${Math.floor(index / 8)}][${Math.floor((index % 8) / 4)}][${(index % 8) % 4}]`;
      } else {
        return `${name}[${Math.floor(index / 4)}][${index % 4}]`;
      }
    }
  } else {
    return length > 1 ? `${name}[${index}]` : name;
  }
};

/**
 * A helper function to get a IndicesHelper for a given input or output.
 *
 * @param name - the name of the input or output.
 * @param tensorType - the tensor type of the input or output.
 * @param shapeOrRank - the tensor shape or the rank of the input or output.
 * @param usage - the usage of the indices helper.
 * @param components - indicates the number of components of each element. 1 for scalar, 2 for vec2, 3 for vec3, 4 for
 *    vec4.
 */
const createIndicesHelper = (
  name: string,
  tensorType: number,
  shapeOrRank: number | readonly number[],
  usage: IndicesHelper['usage'],
  components: 1 | 2 | 3 | 4,
): IndicesHelper => {
  const useUniform = typeof shapeOrRank === 'number';
  const rank = useUniform ? shapeOrRank : shapeOrRank.length;
  const rankIdentity = [...new Array(rank).keys()];
  const indicesType = rank < 2 ? 'u32' : rank <= 4 ? `vec${rank}<u32>` : `array<u32, ${rank}>`;
  const mappedType = getWgslMappedType(tensorType, components);
  const valueType = typeof mappedType === 'string' ? mappedType : mappedType[1];
  const storageType = typeof mappedType === 'string' ? mappedType : mappedType[0];
  const type = { indices: indicesType, value: valueType, storage: storageType, tensor: tensorType };

  const normalizeDim = (dim: number | string): string => (typeof dim === 'string' ? dim : `${dim}u`);

  const implementationUsed = {
    offsetToIndices: false,
    indicesToOffset: false,
    broadcastedIndicesToOffset: false,
    set: false,
    setByIndices: false,
    get: false,
    getByIndices: false,
  };

  const uniformPrefix = useUniform ? 'uniforms.' : '';
  const shape = `${uniformPrefix}${name}_shape`;
  const strides = `${uniformPrefix}${name}_strides`;

  let o2iSnippet = '';
  for (let i = 0; i < rank - 1; i++) {
    o2iSnippet += `
    let dim${i} = current / ${getElementAt(strides, i, rank)};
    let rest${i} = current % ${getElementAt(strides, i, rank)};
    indices[${i}] = dim${i};
    current = rest${i};
    `;
  }
  o2iSnippet += `indices[${rank - 1}] = current;`;

  const offsetToIndicesImplementation =
    rank < 2
      ? ''
      : `
  fn o2i_${name}(offset: u32) -> ${type.indices} {
    var indices: ${type.indices};
    var current = offset;
    ${o2iSnippet}
    return indices;
  }`;

  const offsetToIndices = (varOffset: string) => {
    implementationUsed.offsetToIndices = true;
    return rank < 2 ? varOffset : `o2i_${name}(${varOffset})`;
  };

  const offsets: string[] = [];
  if (rank >= 2) {
    for (let i = rank - 1; i >= 0; i--) {
      offsets.push(`${getElementAt(strides, i, rank)} * (indices[${i}])`);
    }
  }

  const indicesToOffsetImplementation =
    rank < 2
      ? ''
      : `
  fn i2o_${name}(indices: ${type.indices}) -> u32 {
    return ${offsets.join('+')};
  }`;

  const indicesToOffset = (varIndices: string) => {
    implementationUsed.indicesToOffset = true;
    return rank < 2 ? varIndices : `i2o_${name}(${varIndices})`;
  };

  const indices = (...init: ReadonlyArray<number | string>) =>
    rank === 0 ? '0u' : `${type.indices}(${init.map(normalizeDim).join(',')})`;

  const indicesGet = (varIndices: string, idx: number | string) => {
    if (rank < 2) {
      return `${varIndices}`;
    } else {
      return `${getElementAt(varIndices, idx, rank)}`;
    }
  };

  const indicesSet = (varIndices: string, idx: number | string, value: string) => {
    if (rank < 2) {
      return `${varIndices}=${value};`;
    } else {
      return `${getElementAt(varIndices, idx, rank)}=${value};`;
    }
  };

  const broadcastedIndicesToOffsetImplementation: { [key: string]: string } = {};
  const broadcastedIndicesToOffset = (varIndices: string, output: IndicesHelper) => {
    implementationUsed.broadcastedIndicesToOffset = true;
    const implKey = `${output.name}broadcastedIndicesTo${name}Offset`;
    if (implKey in broadcastedIndicesToOffsetImplementation) {
      return `${implKey}(${varIndices})`;
    }
    const offsets = [];
    for (let i = rank - 1; i >= 0; i--) {
      const idx = output.indicesGet('outputIndices', i + output.rank - rank);
      offsets.push(`${indicesGet(strides, i)} * (${idx} % ${indicesGet(shape, i)})`);
    }
    broadcastedIndicesToOffsetImplementation[implKey] = `fn ${implKey}(outputIndices: ${output.type.indices}) -> u32 {
             return ${offsets.length > 0 ? offsets.join('+') : '0u'};
           }`;

    return `${implKey}(${varIndices})`;
  };

  const setByOffset = (offset: number | string, value: string) =>
    (() => {
      if (type.storage === type.value) {
        return `${name}[${offset}]=${value};`;
      } else if (type.storage === 'vec2<u32>' && type.value === 'i32') {
        // int64, components === 1
        return `${name}[${offset}]=vec2<u32>(u32(${value}), select(0u, 0xFFFFFFFFu, ${value} < 0));`;
      } else if (type.storage === 'vec2<u32>' && type.value === 'u32') {
        // uint64, components === 1
        return `${name}[${offset}]=vec2<u32>(u32(${value}), 0u);`;
      } else if (type.storage === 'u32' && type.value === 'vec4<bool>') {
        // bool, components === 4
        return `${name}[${offset}]=dot(vec4<u32>(0x1, 0x100, 0x10000, 0x1000000), vec4<u32>(${value}));`;
      } else {
        throw new Error(`not supported combination of storage type ${type.storage} and value type ${type.value} yet`);
      }
    })();

  const getByOffset = (offset: number | string) =>
    (() => {
      if (type.storage === type.value) {
        return `${name}[${offset}]`;
      } else if (type.storage === 'vec2<u32>' && type.value === 'i32') {
        // int64, components === 1
        return `i32(${name}[${offset}].x)`;
      } else if (type.storage === 'vec2<u32>' && type.value === 'u32') {
        // uint64, components === 1
        return `u32(${name}[${offset}].x)`;
      } else if (type.storage === 'u32' && type.value === 'vec4<bool>') {
        // bool, components === 4
        return `vec4<bool>(bool(${name}[${offset}] & 0xFFu), bool(${name}[${offset}] & 0xFF00u), bool(${name}[${
          offset
        }] & 0xFF0000u), bool(${name}[${offset}] & 0xFF000000u))`;
      } else {
        throw new Error(`not supported combination of storage type ${type.storage} and value type ${type.value} yet`);
      }
    })();

  const getByIndicesImplementation =
    rank < 2
      ? ''
      : `
  fn get_${name}ByIndices(indices: ${type.indices}) -> ${valueType} {
    return ${getByOffset(`i2o_${name}(indices)`)};
  }`;

  const getImplementation =
    rank < 2
      ? ''
      : (() => {
          const functionParams = rankIdentity.map((i) => `d${i}: u32`).join(', ');
          const dimsParams = rankIdentity.map((i) => `d${i}`).join(', ');
          return `
  fn get_${name}(${functionParams}) -> ${valueType} {
    return get_${name}ByIndices(${indices(dimsParams)});
  }`;
        })();

  const get = (...indices: ReadonlyArray<number | string>) => {
    if (indices.length !== rank) {
      throw new Error(`indices length must be ${rank}`);
    }

    const normalizedIndices = indices.map(normalizeDim).join(',');

    if (rank === 0) {
      return getByOffset('0u');
    } else if (rank === 1) {
      return getByOffset(normalizedIndices[0]);
    } else {
      implementationUsed.get = true;
      implementationUsed.getByIndices = true;
      implementationUsed.indicesToOffset = true;
      return `get_${name}(${normalizedIndices})`;
    }
  };

  const getByIndices = (varIndices: string) => {
    if (rank < 2) {
      return getByOffset(varIndices);
    } else {
      implementationUsed.getByIndices = true;
      implementationUsed.indicesToOffset = true;
      return `get_${name}ByIndices(${varIndices})`;
    }
  };

  const setByIndicesImplementation =
    rank < 2
      ? ''
      : `
  fn set_${name}ByIndices(indices: ${type.indices}, value: ${valueType}) {
    ${setByOffset(`i2o_${name}(indices)`, 'value')}
  }`;

  const setImplementation =
    rank < 2
      ? ''
      : (() => {
          const functionParams = rankIdentity.map((i) => `d${i}: u32`).join(', ');
          const dimsParams = rankIdentity.map((i) => `d${i}`).join(', ');
          return `
  fn set_${name}(${functionParams}, value: ${valueType}) {
    set_${name}ByIndices(${indices(dimsParams)}, value);
  }`;
        })();

  const set = (...indicesAndValue: ReadonlyArray<number | string>) => {
    if (indicesAndValue.length !== rank + 1) {
      throw new Error(`indices length must be ${rank}`);
    }
    const value = indicesAndValue[rank];
    if (typeof value !== 'string') {
      throw new Error('value must be string');
    }

    const normalizedIndices = indicesAndValue.slice(0, rank).map(normalizeDim).join(',');

    if (rank === 0) {
      return setByOffset('0u', value);
    } else if (rank === 1) {
      return setByOffset(normalizedIndices[0], value);
    } else {
      implementationUsed.set = true;
      implementationUsed.setByIndices = true;
      implementationUsed.indicesToOffset = true;
      return `set_${name}(${normalizedIndices}, ${value})`;
    }
  };

  const setByIndices = (varIndices: string, value: string) => {
    if (rank < 2) {
      return setByOffset(varIndices, value);
    } else {
      implementationUsed.setByIndices = true;
      implementationUsed.indicesToOffset = true;
      return `set_${name}ByIndices(${varIndices}, ${value});`;
    }
  };

  const impl = () => {
    const impls = [];
    let needShapeStrides = false;
    if (implementationUsed.offsetToIndices) {
      impls.push(offsetToIndicesImplementation);
      needShapeStrides = true;
    }
    if (implementationUsed.indicesToOffset) {
      impls.push(indicesToOffsetImplementation);
      needShapeStrides = true;
    }
    if (implementationUsed.broadcastedIndicesToOffset) {
      Object.values(broadcastedIndicesToOffsetImplementation).forEach((impl) => impls.push(impl));
      needShapeStrides = true;
    }
    if (implementationUsed.set) {
      impls.push(setImplementation);
      needShapeStrides = true;
    }
    if (implementationUsed.setByIndices) {
      impls.push(setByIndicesImplementation);
      needShapeStrides = true;
    }
    if (implementationUsed.get) {
      impls.push(getImplementation);
      needShapeStrides = true;
    }
    if (implementationUsed.getByIndices) {
      impls.push(getByIndicesImplementation);
      needShapeStrides = true;
    }
    if (!useUniform && needShapeStrides) {
      impls.unshift(
        `const ${shape} = ${type.indices}(${shapeOrRank.join(',')});`,
        `const ${strides} = ${type.indices}(${ShapeUtil.computeStrides(shapeOrRank).join(',')});`,
      );
    }
    return impls.join('\n');
  };

  return {
    impl,
    type,
    offsetToIndices,
    indicesToOffset,
    broadcastedIndicesToOffset,
    indices,
    indicesGet,
    indicesSet,
    set,
    setByOffset,
    setByIndices,
    get,
    getByOffset,
    getByIndices,
    // isVec4,
    usage,
    name,
    strides,
    shape,
    rank,
  };
};

/**
 * Create a IndicesHelper for an input.
 *
 * @param name - the name of the input.
 * @param type - the tensor type of the input.
 * @param shapeOrRank - the tensor shape or the rank of the input.
 * @param components - the number of components of the input. available values are 1, 2, 3, 4. default is 1.
 * @returns an IndicesHelper for the input.
 */
export const inputVariable = (
  name: string,
  type: number,
  shapeOrRank: number | readonly number[],
  components: 1 | 2 | 3 | 4 = 1,
): IndicesHelper => createIndicesHelper(name, type, shapeOrRank, 'input', components);

/**
 * Create a IndicesHelper for an output.
 *
 * @param name - the name of the output.
 * @param type - the tensor type of the output.
 * @param shapeOrRank - the tensor shape or the rank of the output.
 * @param components - the number of components of the output. available values are 1, 2, 3, 4. default is 1.
 * @returns an IndicesHelper for the output.
 */
export const outputVariable = (
  name: string,
  type: number,
  shapeOrRank: number | readonly number[],
  components: 1 | 2 | 3 | 4 = 1,
): IndicesHelper => createIndicesHelper(name, type, shapeOrRank, 'output', components);

/**
 * Create a IndicesHelper for an atomic output.
 *
 * @param name - the name of the output.
 * @param type - the tensor type of the output.
 * @param shapeOrRank - the tensor shape or the rank of the output.
 * @returns an IndicesHelper for the output.
 */
export const atomicOutputVariable = (
  name: string,
  type: number,
  shapeOrRank: number | readonly number[],
): IndicesHelper => createIndicesHelper(name, type, shapeOrRank, 'atomicOutput', 1);

/**
 * Create a IndicesHelper for an internal variable.
 *
 * @param name - the name of the variable.
 * @param type - the tensor type of the variable.
 * @param shapeOrRank - the tensor shape or the rank of the variable.
 * @param components - the number of components of the variable. available values are 1, 2, 3, 4. default is 1.
 * @returns an IndicesHelper for the variable.
 */
export const internalVariable = (
  name: string,
  type: number,
  shapeOrRank: number | readonly number[],
  components: 1 | 2 | 3 | 4 = 1,
): IndicesHelper => createIndicesHelper(name, type, shapeOrRank, 'internal', components);

export type UniformDataElementType = 'u32' | 'f16' | 'f32' | 'i32';
export type UniformsArrayType = Array<{ name: string; type: UniformDataElementType; length?: number }>;

/**
 * A ShaderHelper is a helper class for generating WGSL code.
 */
export interface ShaderHelper {
  /**
   * A helper function to generate the start of main function in WGSL source code.
   *
   * @example
   * const getShaderSource = (shaderHelper: ShaderHelper) => `
   *  ...
   *
   *  ${shaderHelper.mainStart()}
   *    // your code here inside main() function
   *    ...
   *  }
   * `;
   *
   * @param workgroupSize - an optional workgroup size. default is WORKGROUP_SIZE.
   */
  mainStart(workgroupSize?: number | [number, number, number]): string;

  /**
   * A helper function to generate the code snippet for guarding against out-of-bounds size.
   *
   * @example
   * const getShaderSource = (shaderHelper: ShaderHelper) => `
   *  ...
   *
   *  ${shaderHelper.mainStart()}
   *    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes(outputSize)}
   *
   *    // your code here inside main() function
   *    ...
   *  }
   * `;
   *
   * @param size - the size of the data to guard against. can be a number or a string (WGSL `u32` expression).
   */
  guardAgainstOutOfBoundsWorkgroupSizes(size: unknown): string;

  /**
   * A helper function to generate the code snippet for declaring multiple inputs or outputs.
   *
   * @param variables - an array of IndicesHelper for the variables.
   */
  declareVariables(...variables: IndicesHelper[]): string;

  /**
   * A helper function to register one uniform. Can be called multiple times to register multiple uniforms.
   *
   * @param name - the name of the uniform.
   * @param type - the type of the uniform.
   * @param length - the length of the uniform, default to 1 when it is not provided.
   */
  registerUniform(name: string, type: string, length?: number): ShaderHelper;

  /**
   * A helper function to register multiple uniforms. Can be called multiple times to register multiple uniforms.
   *
   * @param uniforms - an array of uniforms. Each element of the array is an object with 2 properties: `name` and
   *     `type`.
   */
  registerUniforms(uniforms: UniformsArrayType): ShaderHelper;

  /**
   * A helper function to register multiple internal variables. Can be called multiple times to register multiple
   * internal variables.
   *
   * @param variables - an array of IndicesHelper for the variables.
   */
  registerInternalVariables(...variables: IndicesHelper[]): ShaderHelper;
}

class ShaderHelperImpl implements ShaderHelper {
  constructor(
    private normalizedDispatchGroup: [number, number, number],
    private limits: GPUSupportedLimits,
  ) {}

  guardAgainstOutOfBoundsWorkgroupSizes(size: number | string): string {
    // Guard against out-of-bounds work group sizes
    const sizeInCode = typeof size === 'number' ? `${size}u` : size;
    return `if (global_idx >= ${sizeInCode}) { return; }`;
  }

  mainStart(workgroupSize: number | [number, number, number] = WORKGROUP_SIZE) {
    const workgroupSizeX = typeof workgroupSize === 'number' ? workgroupSize : workgroupSize[0];
    const workgroupSizeY = typeof workgroupSize === 'number' ? 1 : workgroupSize[1];
    const workgroupSizeZ = typeof workgroupSize === 'number' ? 1 : workgroupSize[2];

    if (
      workgroupSizeX > this.limits.maxComputeWorkgroupSizeX ||
      workgroupSizeY > this.limits.maxComputeWorkgroupSizeY ||
      workgroupSizeZ > this.limits.maxComputeWorkgroupSizeZ
    ) {
      throw new Error(
        `workgroup size [${workgroupSizeX}, ${workgroupSizeY}, ${
          workgroupSizeZ
        }] exceeds the maximum workgroup size [${this.limits.maxComputeWorkgroupSizeX}, ${
          this.limits.maxComputeWorkgroupSizeY
        }, ${this.limits.maxComputeWorkgroupSizeZ}].`,
      );
    }

    if (workgroupSizeX * workgroupSizeY * workgroupSizeZ > this.limits.maxComputeInvocationsPerWorkgroup) {
      throw new Error(
        `workgroup size [${workgroupSizeX}, ${workgroupSizeY}, ${
          workgroupSizeZ
        }] exceeds the maximum workgroup invocations ${this.limits.maxComputeInvocationsPerWorkgroup}.`,
      );
    }

    const is1DimensionDispatch = this.normalizedDispatchGroup[1] === 1 && this.normalizedDispatchGroup[2] === 1;
    const paramList = is1DimensionDispatch
      ? `@builtin(global_invocation_id) global_id : vec3<u32>,
    @builtin(workgroup_id) workgroup_id : vec3<u32>,
    @builtin(local_invocation_index) local_idx : u32,
    @builtin(local_invocation_id) local_id : vec3<u32>`
      : `@builtin(global_invocation_id) global_id : vec3<u32>,
                                             @builtin(local_invocation_id) local_id : vec3<u32>,
    @builtin(local_invocation_index) local_idx : u32,
    @builtin(workgroup_id) workgroup_id : vec3<u32>,
    @builtin(num_workgroups) num_workgroups : vec3<u32>`;
    const globalIdxDefinition = is1DimensionDispatch
      ? `let global_idx = global_id.x;
         let workgroup_index = workgroup_id.x;`
      : `let workgroup_index = workgroup_id.z * num_workgroups[0] * num_workgroups[1] +
             workgroup_id.y * num_workgroups[0] + workgroup_id.x;
         let global_idx = workgroup_index * ${workgroupSizeX * workgroupSizeY * workgroupSizeZ}u + local_idx;`;

    return `@compute @workgroup_size(${workgroupSizeX}, ${workgroupSizeY}, ${workgroupSizeZ})
  fn main(${paramList}) {
    ${globalIdxDefinition}
  `;
  }

  private appendVariableUniforms(variable: IndicesHelper): void {
    if (variable.rank !== 0) {
      if (variable.shape.startsWith('uniforms.')) {
        this.uniforms.push({ name: variable.shape.replace('uniforms.', ''), type: 'u32', length: variable.rank });
      }
      if (variable.strides.startsWith('uniforms.')) {
        this.uniforms.push({ name: variable.strides.replace('uniforms.', ''), type: 'u32', length: variable.rank });
      }
    }
  }

  private declareVariable(variable: IndicesHelper, bindingIndex: number): string {
    if (variable.usage === 'internal') {
      throw new Error('cannot use internal variable with declareVariable(). use registerInternalVariables() instead.');
    }
    this.variables.push(variable);
    this.appendVariableUniforms(variable);
    const access = variable.usage === 'input' ? 'read' : 'read_write';
    const storageType = variable.usage === 'atomicOutput' ? `atomic<i32>` : variable.type.storage;
    return `@group(0) @binding(${bindingIndex}) var<storage, ${access}> ${variable.name}: array<${storageType}>;`;
  }

  declareVariables(...variables: IndicesHelper[]): string {
    return variables.map((v) => this.declareVariable(v, this.variableIndex++)).join('\n');
  }

  private registerInternalVariable(variable: IndicesHelper): void {
    if (variable.usage !== 'internal') {
      throw new Error(
        'cannot use input or output variable with registerInternalVariable(). use declareVariables() instead.',
      );
    }

    this.internalVariables.push(variable);
    this.appendVariableUniforms(variable);
  }

  registerInternalVariables(...variables: IndicesHelper[]): ShaderHelper {
    variables.forEach((v) => this.registerInternalVariable(v));
    return this;
  }

  registerUniform(name: string, type: UniformDataElementType, length = 1): ShaderHelper {
    this.uniforms.push({ name, type, length });
    return this;
  }

  registerUniforms(additionalUniforms: UniformsArrayType): ShaderHelper {
    this.uniforms = this.uniforms.concat(additionalUniforms);
    return this;
  }

  private internalVariables: IndicesHelper[] = [];
  private variables: IndicesHelper[] = [];
  private uniforms: UniformsArrayType = [];
  private uniformDeclaration(): string {
    if (this.uniforms.length === 0) {
      return '';
    }

    const uniformSnippets: string[] = [];
    for (const { name, type, length } of this.uniforms) {
      if (length && length > 4) {
        if (type === 'f16') {
          uniformSnippets.push(`@align(16) ${name}:array<mat2x4<${type}>, ${Math.ceil(length / 8)}>`);
        } else {
          uniformSnippets.push(`${name}:array<vec4<${type}>, ${Math.ceil(length / 4)}>`);
        }
      } else {
        const typeTemp = length == null || length === 1 ? type : `vec${length}<${type}>`;
        uniformSnippets.push(`${name}:${typeTemp}`);
      }
    }

    return `
      struct Uniforms { ${uniformSnippets.join(', ')} };
      @group(0) @binding(${this.variableIndex}) var<uniform> uniforms: Uniforms;`;
  }
  private variableIndex = 0;

  /**
   * Get additional implementation that needs to be added to the shader source.
   */
  get additionalImplementations(): string {
    return (
      this.uniformDeclaration() +
      this.variables.map((i) => i.impl()).join('\n') +
      this.internalVariables.map((i) => i.impl()).join('\n')
    );
  }

  /**
   * Get the variable info of the shader program.
   */
  get variablesInfo(): ProgramUniformVariableInfo[] | undefined {
    if (this.uniforms.length === 0) {
      return undefined;
    }

    const uniformWgslTypeToDataType = (type: UniformDataElementType) =>
      [DataType.uint32, DataType.float16, DataType.float, DataType.int32][['u32', 'f16', 'f32', 'i32'].indexOf(type)];
    return this.uniforms.map((u) => [uniformWgslTypeToDataType(u.type), u.length ?? 1]);
  }
}

export const createShaderHelper = (dispatchGroup: [number, number, number], limits: GPUSupportedLimits) =>
  new ShaderHelperImpl(dispatchGroup, limits);
