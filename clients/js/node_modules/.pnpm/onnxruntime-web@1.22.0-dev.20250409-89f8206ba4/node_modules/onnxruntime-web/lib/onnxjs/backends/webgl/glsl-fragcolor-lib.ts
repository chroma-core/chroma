// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { GlslContext, GlslLib, GlslLibRoutine } from './glsl-definitions';
import { getGlsl } from './glsl-source';

/**
 * This GLSL library handles routines around reading a texlet and writing to it
 * Reading and writing could be more than just dealing with one channel
 * It may require encoding/decoding to/from 4 channels into one
 */
export class FragColorGlslLib extends GlslLib {
  constructor(context: GlslContext) {
    super(context);
  }
  getFunctions(): { [name: string]: GlslLibRoutine } {
    return { ...this.setFragColor(), ...this.getColorAsFloat() };
  }
  getCustomTypes(): { [name: string]: string } {
    return {};
  }
  protected setFragColor(): { [name: string]: GlslLibRoutine } {
    const glsl = getGlsl(this.context.glContext.version);
    return {
      setFragColor: new GlslLibRoutine(
        `
        void setFragColor(float value) {
            ${glsl.output} = encode(value);
        }
        `,
        ['encoding.encode'],
      ),
    };
  }
  protected getColorAsFloat(): { [name: string]: GlslLibRoutine } {
    return {
      getColorAsFloat: new GlslLibRoutine(
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
