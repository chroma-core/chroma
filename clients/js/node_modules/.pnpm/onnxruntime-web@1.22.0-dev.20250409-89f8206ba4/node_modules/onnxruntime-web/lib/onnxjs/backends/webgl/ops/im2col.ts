// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

import { ConvAttributes } from './conv';

const createIm2ColProgramMetadata = (cacheHint: string) => ({
  name: 'Im2Col',
  inputNames: ['X'],
  inputTypes: [TextureType.unpacked],
  cacheHint,
});

const createIm2ColProgramInfo = (
  _inferenceHandler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  x: Tensor,
  w: Tensor,
  outputShape: readonly number[],
  attributes: ConvAttributes,
): ProgramInfo => {
  const xshape = x.dims;
  const wshape = w.dims;

  const rank = outputShape.length;
  const im2colDims = calculateIm2ColDims(xshape, wshape, outputShape, 4);

  const shaderSource = `
        const int XC = ${xshape[1]};
        const int XH = ${xshape[2]};
        const int XW = ${xshape[3]};
        const int KH = ${attributes.kernelShape[0]};
        const int KW = ${attributes.kernelShape[1]};
        const int dilationH = ${attributes.dilations[0]};
        const int dilationW = ${attributes.dilations[1]};
        const int strideH = ${attributes.strides[0]};
        const int strideW = ${attributes.strides[1]};
        const int padH = ${attributes.pads[0]};
        const int padW = ${attributes.pads[1]};
        const int KHKW = KH*KW;
        const int XCKHKW = XC * KHKW;
        const int outputChannels = 4;
        vec4 process(int indices[${rank}]) {
          int b  = indices[0]; // batch size
          int oh = indices[1] * strideH - padH; //output height
          int ow = indices[2] * strideW - padW; //output width
          int p = indices[3] * outputChannels; //patch
          vec4 value = vec4(0.0);
          for(int i=0; i < outputChannels; ++i) {
            if(p < XCKHKW) {
              int patchC = p / KHKW;
              int patchH = (p - patchC*KHKW) / KW;
              int patchW = (p - patchC*KHKW) - patchH * KW;
              int xh2 = oh + patchH * dilationH;
              int xw2 = ow + patchW * dilationW;
              int x[${xshape.length}];
              x[0] = b;
              x[1] = patchC;
              x[2] = xh2;
              x[3] = xw2;
              if(xh2 >= 0 &&
                  xh2 < XH &&
                  xw2 >= 0 &&
                  xw2 < XW) {
                value[i] = _X(x);
              }
            }
            ++p;
          }
          return value;
        }
        `;
  return {
    ...metadata,
    output: { dims: im2colDims, type: x.type, textureType: TextureType.packedLastDimension },
    shaderSource,
  };
};

export const createIm2ColProgramInfoLoader = (
  inferenceHandler: WebGLInferenceHandler,
  x: Tensor,
  w: Tensor,
  outputShape: readonly number[],
  attributes: ConvAttributes,
): ProgramInfoLoader => {
  const metadata = createIm2ColProgramMetadata(attributes.cacheKey);
  return {
    ...metadata,
    get: () => createIm2ColProgramInfo(inferenceHandler, metadata, x, w, outputShape, attributes),
  };
};

export const calculateIm2ColDims = (
  inputShape: readonly number[],
  kernelShape: readonly number[],
  outputShape: readonly number[],
  channels = 4,
): number[] => [
  outputShape[0],
  outputShape[2],
  outputShape[3],
  Math.ceil((inputShape[1] * kernelShape[2] * kernelShape[3]) / channels),
];
