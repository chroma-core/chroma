// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import { createTensorShapeVariables, IndicesHelper, inputVariable, outputVariable, ShaderHelper } from './common';

let [idxN, idxC, idxH, idxW] = [0, 1, 2, 3]; // NCHW
type Mode = 'bilinear' | 'nearest' | 'bicubic';
type PaddingMode = 'zeros' | 'border' | 'reflection';
type Format = 'NHWC' | 'NCHW';
export interface GridSampeAttributes extends AttributeWithCacheKey {
  alignCorners: number;
  mode: Mode;
  paddingMode: PaddingMode;
  format: Format;
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (inputs[0].dims.length !== 4) {
    throw new Error('only 4-D tensor is supported.');
  }
  if (inputs[0].dims.length !== inputs[1].dims.length) {
    throw new Error('input dimensions must be equal to grid dimensions');
  }

  if (inputs[0].dims.length - 2 !== inputs[1].dims[inputs[1].dims.length - 1]) {
    throw new Error(`last dimension of grid must be equal to ${inputs[0].dims.length - 2}`);
  }

  if (inputs[0].dims[0] !== inputs[1].dims[0]) {
    throw new Error('grid batch size must match input batch size');
  }
};

const gsGetCubicCoeffs = `
  fn gs_get_cubic_coeffs(x: f32) -> vec4<f32> {
    let cubic_alpha = -0.75f;
    let x_abs = abs(x);
    var coeffs: vec4<f32>;
    coeffs[0] = (((cubic_alpha * (x_abs + 1) - 5 * cubic_alpha) * (x_abs + 1) + 8 * cubic_alpha) * (x_abs + 1) - 4 * cubic_alpha);
    coeffs[1] = (((cubic_alpha + 2) * x_abs - (cubic_alpha + 3)) * x_abs * x_abs + 1);
    coeffs[2] = (((cubic_alpha + 2) * (1 - x_abs) - (cubic_alpha + 3)) * (1 - x_abs) * (1 - x_abs) + 1);
    coeffs[3] = (((cubic_alpha * (2 - x_abs) - 5 * cubic_alpha) * (2 - x_abs) + 8 * cubic_alpha) * (2 - x_abs) - 4 * cubic_alpha);
    return coeffs;
  }
`;

const gsBicubicInterpolate = (dataType: string): string => `
  fn gs_bicubic_interpolate(p: mat4x4<${dataType}>, x: f32, y: f32) -> ${dataType} {
    var v: vec4<f32>;
    var coeffs = gs_get_cubic_coeffs(x);
    for (var i = 0; i < 4; i++) {
      v[i] = coeffs[0] * p[i][0] + coeffs[1] * p[i][1] + coeffs[2] * p[i][2] + coeffs[3] * p[i][3];
    }
    coeffs = gs_get_cubic_coeffs(y);
    let pixel = ${dataType}(coeffs[0] * v[0] + coeffs[1] * v[1] + coeffs[2] * v[2] + coeffs[3] * v[3]);
    return pixel;
  }
`;

const gsDenormalize = (attributes: GridSampeAttributes): string => `
  fn gs_denormalize(n: f32, length: i32) -> f32 {
    ${
      attributes.alignCorners === 0
        ? `
    // alignCorners: false => [-1, 1] to [-0.5, length - 0.5]
    return ((n + 1.0) * f32(length) - 1.0) / 2.0;
    `
        : `
    // alignCorners: true => [-1, 1] to [0, length - 1]
    return (n + 1.0) / 2.0 * (f32(length - 1));
    `
    }
  }
`;

const gsReflect = (attributes: GridSampeAttributes): string => `
  ${
    attributes.paddingMode === 'reflection'
      ? `
      fn gs_reflect(x: i32, x_min: f32, x_max: f32) -> u32 {
        var dx = 0.0;
        var fx = f32(x);
        let range = x_max - x_min;
        if (fx < x_min) {
          dx = x_min - fx;
          let n = u32(dx / range);
          let r = dx - f32(n) * range;
          if (n % 2 == 0) {
            fx = x_min + r;
          } else {
            fx = x_max - r;
          }
        } else if (fx > x_max) {
          dx = fx - x_max;
          let n = u32(dx / range);
          let r = dx - f32(n) * range;
          if (n % 2 == 0) {
            fx = x_max - r;
          } else {
            fx = x_min + r;
          }
        }
        return u32(fx);
      }`
      : ''
  }
`;

const pixelAtGrid = (input: IndicesHelper, dataType: string, attributes: GridSampeAttributes): string =>
  `
  fn pixel_at_grid(r: i32, c: i32, H: i32, W: i32, batch: u32, channel: u32, border: vec4<f32>) -> ${dataType} {
     var pixel = ${dataType}(0);
     var indices = vec4<u32>(0);
     indices[${idxN}] = batch;
     indices[${idxC}] = channel;` +
  (() => {
    switch (attributes.paddingMode) {
      case 'zeros':
        return `
          if (r >= 0 && r < H && c >=0 && c < W) {
            indices[${idxH}] = u32(r);
            indices[${idxW}] = u32(c);
          } else {
            return ${dataType}(0);
          }
        `;
      case 'border':
        return `
          indices[${idxH}] = u32(clamp(r, 0, H - 1));
          indices[${idxW}] = u32(clamp(c, 0, W - 1));
        `;
      case 'reflection':
        return `
          indices[${idxH}] = gs_reflect(r, border[1], border[3]);
          indices[${idxW}] = gs_reflect(c, border[0], border[2]);
        `;
      default:
        throw new Error(`padding mode ${attributes.paddingMode} is not supported`);
    }
  })() +
  `
    return ${input.getByIndices('indices')};
  }
`;

const computePixel = (output: IndicesHelper, dataType: string, attributes: GridSampeAttributes): string =>
  (() => {
    switch (attributes.mode) {
      case 'nearest':
        return `
          let result = pixel_at_grid(i32(round(y)), i32(round(x)), H_in, W_in, indices[${idxN}], indices[${idxC}], border);
        `;
      case 'bilinear':
        return `
          let x1 = i32(floor(x));
          let y1 = i32(floor(y));
          let x2 = x1 + 1;
          let y2 = y1 + 1;

          let p11 = pixel_at_grid(y1, x1, H_in, W_in, indices[${idxN}], indices[${idxC}], border);
          let p12 = pixel_at_grid(y1, x2, H_in, W_in, indices[${idxN}], indices[${idxC}], border);
          let p21 = pixel_at_grid(y2, x1, H_in, W_in, indices[${idxN}], indices[${idxC}], border);
          let p22 = pixel_at_grid(y2, x2, H_in, W_in, indices[${idxN}], indices[${idxC}], border);

          let dx2 = ${dataType}(f32(x2) - x);
          let dx1 = ${dataType}(x - f32(x1));
          let dy2 = ${dataType}(f32(y2) - y);
          let dy1 = ${dataType}(y - f32(y1));
          let result = dy2 * (dx2 * p11 + dx1 * p12) + dy1 * (dx2 * p21 + dx1 * p22);
        `;
      case 'bicubic':
        return `
          let x0 = i32(floor(x)) - 1;
          let y0 = i32(floor(y)) - 1;
          var p: mat4x4<${dataType}>;
          for (var h = 0; h < 4; h++) {
            for (var w = 0; w < 4; w++) {
              p[h][w] = pixel_at_grid(h + y0, w + x0, H_in, W_in, indices[${idxN}], indices[${idxC}], border);
            }
          }

          let dx = x - f32(x0 + 1);
          let dy = y - f32(y0 + 1);
          let result = gs_bicubic_interpolate(p, dx, dy);
        `;
      default:
        throw new Error(`mode ${attributes.mode} is not supported`);
    }
  })() + `${output.setByOffset('global_idx', 'result')}`;

const createGridSampleProgramInfo = (inputs: readonly TensorView[], attributes: GridSampeAttributes): ProgramInfo => {
  const x = inputVariable('x', inputs[0].dataType, inputs[0].dims.length);
  // discard last dimension for using vec2 to access grid data
  const gridShape = [inputs[1].dims[0], inputs[1].dims[1], inputs[1].dims[2]];
  const grid = inputVariable('grid', inputs[1].dataType, gridShape.length, 2);
  let outputShape = [inputs[0].dims[0], inputs[0].dims[1], inputs[1].dims[1], inputs[1].dims[2]];
  if (attributes.format === 'NHWC') {
    outputShape = [inputs[0].dims[0], inputs[1].dims[1], inputs[1].dims[2], inputs[0].dims[3]];
    [idxN, idxC, idxH, idxW] = [0, 3, 1, 2];
  }
  const output = outputVariable('output', inputs[0].dataType, outputShape.length);
  const dataType = x.type.value;
  const outputSize = ShapeUtil.size(outputShape);

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    ...createTensorShapeVariables(inputs[0].dims, gridShape, outputShape),
  ];

  const getShaderSource = (shaderHelper: ShaderHelper) => `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(x, grid, output)}
  ${gsGetCubicCoeffs}
  ${gsBicubicInterpolate(dataType)}
  ${gsDenormalize(attributes)}
  ${gsReflect(attributes)}
  ${pixelAtGrid(x, dataType, attributes)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
      let H_in = i32(uniforms.x_shape[${idxH}]);
      let W_in = i32(uniforms.x_shape[${idxW}]);

      ${
        attributes.alignCorners === 0
          ? `
      let x_min = -0.5;
      let x_max = f32(W_in) - 0.5;
      let y_min = -0.5;
      let y_max = f32(H_in) - 0.5;
      `
          : `
      let x_min = 0.0;
      let x_max = f32(W_in) - 1.0;
      let y_min = 0.0;
      let y_max = f32(H_in) - 1.0;
      `
      };
      let border = vec4<f32>(x_min, y_min, x_max, y_max);

      let indices = ${output.offsetToIndices('global_idx')};
      var grid_indices = vec3<u32>(indices[${idxN}], indices[${idxH}], indices[${idxW}]);
      let nxy = ${grid.getByIndices('grid_indices')};
      var x = gs_denormalize(f32(nxy[0]), W_in);
      var y = gs_denormalize(f32(nxy[1]), H_in);

      ${computePixel(output, dataType, attributes)}
  }`;

  return {
    name: 'GridSample',
    shaderCache: { hint: `${attributes.cacheKey}`, inputDependencies: ['type', 'type'] },
    getRunData: (inputs) => {
      const outputSize = ShapeUtil.size(outputShape);
      return {
        outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms,
      };
    },
    getShaderSource,
  };
};

export const gridSample = (context: ComputeContext, attributes: GridSampeAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createGridSampleProgramInfo(context.inputs, attributes));
};

export const parseGridSampleAttributes = (attributes: Record<string, unknown>): GridSampeAttributes =>
  createAttributeWithCacheKey({
    alignCorners: attributes.align_corners as number,
    mode: attributes.mode as Mode,
    paddingMode: attributes.padding_mode as PaddingMode,
    format: attributes.format as Format,
  });
