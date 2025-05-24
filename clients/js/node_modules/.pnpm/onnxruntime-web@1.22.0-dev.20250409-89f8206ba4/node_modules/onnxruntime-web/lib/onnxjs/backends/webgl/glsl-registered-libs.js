'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.glslRegistry = void 0;
const glsl_coordinate_lib_1 = require('./glsl-coordinate-lib');
const glsl_encoding_lib_1 = require('./glsl-encoding-lib');
const glsl_fragcolor_lib_1 = require('./glsl-fragcolor-lib');
const glsl_shape_utils_lib_1 = require('./glsl-shape-utils-lib');
const glsl_vec_lib_1 = require('./glsl-vec-lib');
exports.glslRegistry = {
  encoding: glsl_encoding_lib_1.EncodingGlslLib,
  fragcolor: glsl_fragcolor_lib_1.FragColorGlslLib,
  vec: glsl_vec_lib_1.VecGlslLib,
  shapeUtils: glsl_shape_utils_lib_1.ShapeUtilsGlslLib,
  coordinates: glsl_coordinate_lib_1.CoordsGlslLib,
  //  'arrays': ArrayGlslSLib
};
//# sourceMappingURL=glsl-registered-libs.js.map
