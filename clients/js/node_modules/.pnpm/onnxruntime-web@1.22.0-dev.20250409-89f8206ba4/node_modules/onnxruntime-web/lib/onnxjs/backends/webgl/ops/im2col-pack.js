'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createPackedIm2ColProgramInfoLoader = void 0;
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const packing_utils_1 = require('./packing-utils');
const createPackedIm2ColProgramMetadata = (cacheHint) => ({
  name: 'Im2Col (packed)',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.packed],
  cacheHint,
});
const createPackedIm2ColProgramInfo = (inferenceHandler, metadata, x, w, outputShape, attributes) => {
  const xshape = x.dims;
  const wshape = w.dims;
  const rowDim = 2;
  const colDim = 3;
  const rank = outputShape.length;
  const im2colShape = [wshape[1] * wshape[2] * wshape[3], outputShape[2] * outputShape[3]];
  const kernelSize = wshape[2] * wshape[3];
  const unpackChannel = (0, packing_utils_1.unpackFromChannel)();
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  let unrolled = '';
  for (let row = 0; row <= 1; row++) {
    for (let col = 0; col <= 1; col++) {
      unrolled += `
            blockIndex = rc.x + ${col};
            pos = rc.y + ${row};

            if(blockIndex < ${im2colShape[1]} && pos < ${im2colShape[0]}) {
              offsetY = int(blockIndex / (${outputShape[rank - 1]})) * ${attributes.strides[0]} -
                ${attributes.pads[0]};
              d0 = offsetY + ${attributes.dilations[0]} * (imod(pos, ${kernelSize}) / ${wshape[2]});

              if(d0 < ${xshape[rowDim]} && d0 >= 0) {
                offsetX = imod(blockIndex, ${outputShape[rank - 1]}) * ${attributes.strides[1]} -
                  ${attributes.pads[1]};
                d1 = offsetX + ${attributes.dilations[1]} * imod(imod(pos, ${kernelSize}), ${wshape[2]});

                if(d1 < ${xshape[colDim]} && d1 >= 0) {

                  ch = int(float(pos)/ ${kernelSize}.);
                    innerDims = vec2(d0, d1);
                    result[${row * 2 + col}] = getChannel(
                      getA(0, ch, int(innerDims.x),
                      int(innerDims.y)), innerDims);
                }
              }
            }

          `;
    }
  }
  const shaderSource = `
      ${unpackChannel}

      void main() {
        ivec2 rc = getOutputCoords();
          vec4 result = vec4(0.0);
          int blockIndex, pos, offsetY, d0, offsetX, d1, ch;
          vec2 innerDims;
          ${unrolled}
          ${glsl.output} = result;
      }
            `;
  return {
    ...metadata,
    output: { dims: im2colShape, type: x.type, textureType: types_1.TextureType.packed },
    shaderSource,
    hasMain: true,
  };
};
const createPackedIm2ColProgramInfoLoader = (inferenceHandler, x, w, outputShape, attributes) => {
  const metadata = createPackedIm2ColProgramMetadata(attributes.cacheKey);
  return {
    ...metadata,
    get: () => createPackedIm2ColProgramInfo(inferenceHandler, metadata, x, w, outputShape, attributes),
  };
};
exports.createPackedIm2ColProgramInfoLoader = createPackedIm2ColProgramInfoLoader;
//# sourceMappingURL=im2col-pack.js.map
