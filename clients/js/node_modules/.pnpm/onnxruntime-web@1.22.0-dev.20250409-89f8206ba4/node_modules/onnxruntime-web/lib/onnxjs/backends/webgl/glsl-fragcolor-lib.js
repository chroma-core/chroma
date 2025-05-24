'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.FragColorGlslLib = void 0;
const glsl_definitions_1 = require('./glsl-definitions');
const glsl_source_1 = require('./glsl-source');
/**
 * This GLSL library handles routines around reading a texlet and writing to it
 * Reading and writing could be more than just dealing with one channel
 * It may require encoding/decoding to/from 4 channels into one
 */
class FragColorGlslLib extends glsl_definitions_1.GlslLib {
  constructor(context) {
    super(context);
  }
  getFunctions() {
    return { ...this.setFragColor(), ...this.getColorAsFloat() };
  }
  getCustomTypes() {
    return {};
  }
  setFragColor() {
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    return {
      setFragColor: new glsl_definitions_1.GlslLibRoutine(
        `
        void setFragColor(float value) {
            ${glsl.output} = encode(value);
        }
        `,
        ['encoding.encode'],
      ),
    };
  }
  getColorAsFloat() {
    return {
      getColorAsFloat: new glsl_definitions_1.GlslLibRoutine(
        `
        float getColorAsFloat(vec4 color) {
            return decode(color);
        }
        `,
        ['encoding.decode'],
      ),
    };
  }
}
exports.FragColorGlslLib = FragColorGlslLib;
//# sourceMappingURL=glsl-fragcolor-lib.js.map
