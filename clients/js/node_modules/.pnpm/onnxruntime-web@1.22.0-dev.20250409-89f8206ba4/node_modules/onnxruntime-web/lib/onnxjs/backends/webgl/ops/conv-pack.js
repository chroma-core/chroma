'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.conv2DPacked = exports.conv2DPackedPointwise = void 0;
const conv_1 = require('./conv');
const im2col_pack_1 = require('./im2col-pack');
const matmul_pack_1 = require('./matmul-pack');
const conv2DPackedPointwise = (inferenceHandler, inputs, attributes) => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const outputShape = (0, conv_1.calculateOutputShape)(
    xshape,
    kshape,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
  );
  const reshapedX = inferenceHandler.reshapePacked(inputs[0], [xshape[1], xshape[2] * xshape[3]]);
  const reshapedK = inferenceHandler.reshapePacked(inputs[1], [kshape[0], kshape[1]]);
  const matmulInputs = inputs.length > 2 ? [reshapedK, reshapedX, inputs[2]] : [reshapedK, reshapedX];
  const matmulOutput = inferenceHandler.run(
    (0, matmul_pack_1.createPackedMatmulProgramInfoLoader)(inferenceHandler, matmulInputs, attributes),
    matmulInputs,
  );
  return inferenceHandler.reshapePacked(matmulOutput, outputShape);
};
exports.conv2DPackedPointwise = conv2DPackedPointwise;
const conv2DPacked = (inferenceHandler, inputs, attributes) => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const outputShape = (0, conv_1.calculateOutputShape)(
    xshape,
    kshape,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
  );
  // run im2col
  const im2colOutput = inferenceHandler.run(
    (0, im2col_pack_1.createPackedIm2ColProgramInfoLoader)(
      inferenceHandler,
      inputs[0],
      inputs[1],
      outputShape,
      attributes,
    ),
    [inputs[0]],
  );
  // reshape kernel
  const kernelReshaped = inferenceHandler.reshapePacked(inputs[1], [kshape[0], kshape[1] * kshape[2] * kshape[3]]);
  // run matmul
  const matmulInputs = inputs.length === 3 ? [kernelReshaped, im2colOutput, inputs[2]] : [kernelReshaped, im2colOutput];
  const matmulOutput = inferenceHandler.run(
    (0, matmul_pack_1.createPackedMatmulProgramInfoLoader)(inferenceHandler, matmulInputs, attributes),
    matmulInputs,
  );
  // reshape output
  const outputReshaped = inferenceHandler.reshapePacked(matmulOutput, outputShape);
  return outputReshaped;
};
exports.conv2DPacked = conv2DPacked;
//# sourceMappingURL=conv-pack.js.map
