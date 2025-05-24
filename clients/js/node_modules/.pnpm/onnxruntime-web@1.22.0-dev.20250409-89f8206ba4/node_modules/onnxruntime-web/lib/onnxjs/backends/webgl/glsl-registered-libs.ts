// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { CoordsGlslLib } from './glsl-coordinate-lib';
import { GlslContext, GlslLib } from './glsl-definitions';
import { EncodingGlslLib } from './glsl-encoding-lib';
import { FragColorGlslLib } from './glsl-fragcolor-lib';
import { ShapeUtilsGlslLib } from './glsl-shape-utils-lib';
import { VecGlslLib } from './glsl-vec-lib';

export const glslRegistry: { [name: string]: new (context: GlslContext) => GlslLib } = {
  encoding: EncodingGlslLib,
  fragcolor: FragColorGlslLib,
  vec: VecGlslLib,
  shapeUtils: ShapeUtilsGlslLib,
  coordinates: CoordsGlslLib,
  //  'arrays': ArrayGlslSLib
};
