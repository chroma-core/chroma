// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

import { unpackFromChannel } from './packing-utils';

const createPackedReshape3DProgramMetadata = (outputShape3D: readonly number[]) => ({
  name: 'Reshape (packed)',
  inputTypes: [TextureType.packed],
  inputNames: ['A'],
  cacheHint: `${outputShape3D}`,
});

const createPackedReshape3DProgramInfo = (
  handler: WebGLInferenceHandler,
  input3D: Tensor,
  metadata: ProgramMetadata,
  outputShape3D: readonly number[],
): ProgramInfo => {
  const inputShape3D = input3D.dims as [number, number, number];
  const squeezedOutputShape = outputShape3D as [number, number, number];

  let mainLoop = '';
  for (let i = 0; i < 4; i++) {
    let outputCoords = '';
    switch (i) {
      case 0:
        outputCoords = 'outputCoords = rc;';
        break;
      case 1:
        outputCoords = 'outputCoords = ivec3(rc.x, rc.y+1, rc.z);';
        break;
      case 2:
        outputCoords = 'outputCoords = ivec3(rc.x, rc.y, rc.z+1);';
        break;
      case 3:
        outputCoords = 'outputCoords = ivec3(rc.x, rc.y+1, rc.z+1);';
        break;
      default:
        throw new Error();
    }

    mainLoop += `
        ${outputCoords}
        ${i > 0 ? 'if(outputCoords.y < rows && outputCoords.z < cols){' : ''}
          int flattenedIndex = getFlattenedIndex(outputCoords);

          ivec3 inputRC = inputCoordsFromReshapedOutCoords(flattenedIndex);
          vec2 innerDims = vec2(float(inputRC.y),float(inputRC.z));

          result[${i}] = getChannel(getA(inputRC.x, inputRC.y, inputRC.z), innerDims);

        ${i > 0 ? '}' : ''}
      `;
  }
  const glsl = getGlsl(handler.session.backend.glContext.version);

  const shaderSource = `
      ${getReshapedInputCoords(inputShape3D)}
      ${getFlattenedIndexFrom3D(squeezedOutputShape)}
      ${unpackFromChannel()}

      void main() {
        ivec3 rc = getOutputCoords();

        vec4 result = vec4(0.0);

        ivec3 outputCoords;
        int rows = ${squeezedOutputShape[2]};
        int cols = ${squeezedOutputShape[1]};

        ${mainLoop}
        ${glsl.output} = result;
      }
    `;

  return {
    ...metadata,
    output: { dims: squeezedOutputShape, type: input3D.type, textureType: TextureType.packed },
    shaderSource,
    hasMain: true,
  };
};

export const createPackedReshape3DProgramInfoLoader = (
  handler: WebGLInferenceHandler,
  input3D: Tensor,
  outputShape3D: readonly number[],
): ProgramInfoLoader => {
  const metadata = createPackedReshape3DProgramMetadata(outputShape3D);
  return { ...metadata, get: () => createPackedReshape3DProgramInfo(handler, input3D, metadata, outputShape3D) };
};

export function processDims3D(shape: ArrayLike<number>): [number, number, number] {
  if (shape.length === 0) {
    return [1, 1, 1];
  }
  // TODO: squeeze other shapes to 2D case
  let batch = 1;
  for (let i = 0; i < shape.length - 2; ++i) {
    batch *= shape[i];
  }
  return [batch, shape.length > 1 ? shape[shape.length - 2] : 1, shape[shape.length - 1]];
}

// For packed reshape, we need to re-arrange texel data for output shape.
// Our pack is designed to pack a 2x2 tile in last h and w dimension, so
// for the reshaped new tensor, we just need to re-arrange the last h and
// w dimension. For any shape that is not in 3D, i.e. [batch, W, H], we
// first convert it to 3D by collapsing other dimension to batch dim, then
// process with the last two dimensions.
// Note: we only need the shape tensor to calculate output shape, so the
// content in shape tensor is never uploaded to GPU. It is always kept in CPU.
// TODO: optimize the algorithm -- in some cases, if the last two dims are
// the same between input shape and output shape, the packed reshape can be
// treated as no-op.
export function isReshapeCheap(dims: readonly number[], reshapedDims: readonly number[]) {
  let isCheapReshape = false;
  if (dims.length === 0 || reshapedDims.length === 0) {
    // scalar
    isCheapReshape = true;
  } else if (dims.length < 2 || reshapedDims.length < 2) {
    // 1D
    isCheapReshape = dims[dims.length - 1] === reshapedDims[reshapedDims.length - 1];
  } else {
    // 2D +
    isCheapReshape =
      dims[dims.length - 1] === reshapedDims[reshapedDims.length - 1] &&
      dims[dims.length - 2] === reshapedDims[reshapedDims.length - 2];
  }

  return isCheapReshape;
}

function getReshapedInputCoords(shape: [number, number, number]): string {
  const strides = ShapeUtil.computeStrides(shape);
  const coords = ['b', 'r', 'c'];
  const index = 'index';
  const coordsFromIndexSnippet = strides
    .map((stride, i) => {
      const line1 = `int ${coords[i]} = ${index} / ${stride}`;
      const line2 =
        i === strides.length - 1
          ? `int ${coords[i + 1]} = ${index} - ${coords[i]} * ${stride}`
          : `index -= ${coords[i]} * ${stride}`;
      return `${line1}; ${line2};`;
    })
    .join('');

  return `
    ivec3 inputCoordsFromReshapedOutCoords(int index) {
      ${coordsFromIndexSnippet}
      return ivec3(b, r, c);
    }
  `;
}

function getFlattenedIndexFrom3D(shape: [number, number, number]): string {
  const strides = ShapeUtil.computeStrides(shape);

  return `
  int getFlattenedIndex(ivec3 coords) {
    // reverse y, z order
    return coords.x * ${strides[0]} + coords.z * ${strides[1]} + coords.y;
  }
`;
}
