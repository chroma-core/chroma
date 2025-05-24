// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { GlslContext, GlslLib, GlslLibRoutine } from './glsl-definitions';

/**
 * GLSL Library responsible for vec routines
 * Vec is an varible length int array. The length is fixed at the time of
 * generating the library functions from the dimensions of the output.
 */
export class VecGlslLib extends GlslLib {
  constructor(context: GlslContext) {
    super(context);
  }
  getCustomTypes(): { [name: string]: string } {
    return {};
  }
  getFunctions(): { [name: string]: GlslLibRoutine } {
    return { ...this.binaryVecFunctions(), ...this.copyVec(), ...this.setVecItem(), ...this.getVecItem() };
  }
  protected binaryVecFunctions(): { [name: string]: GlslLibRoutine } {
    const outputLayout = this.context.outputTextureLayout;
    const rank = outputLayout.shape.length;
    const nameOp: { [name: string]: string } = { add: '+=', sub: '-=', mul: '*=', div: '/=' };
    const result: { [name: string]: GlslLibRoutine } = {};
    for (const name in nameOp) {
      const fname = `${name}Vec`;
      let assignmentBlock = '';
      for (let i = 0; i < rank; ++i) {
        assignmentBlock += `
          dest[${i}] ${nameOp[name]} src[${i}];
          `;
      }
      const body = `
        void ${fname}(int src[${rank}], out int dest[${rank}]) {
          ${assignmentBlock}
        }
        `;
      result[fname] = new GlslLibRoutine(body);
    }

    return result;
  }
  protected copyVec(): { [name: string]: GlslLibRoutine } {
    const outputLayout = this.context.outputTextureLayout;
    const rank = outputLayout.shape.length;
    let assignmentBlock = '';
    for (let i = 0; i < rank; ++i) {
      assignmentBlock += `
        dest[${i}] = src[${i}];
        `;
    }
    const body = `
      void copyVec(int src[${rank}], out int dest[${rank}]) {
        ${assignmentBlock}
      }
      `;
    return { copyVec: new GlslLibRoutine(body) };
  }

  protected setVecItem(): { [name: string]: GlslLibRoutine } {
    const outputLayout = this.context.outputTextureLayout;
    const rank = outputLayout.shape.length;
    let block = `
        if(index < 0)
            index =${rank} + index;
        if (index == 0)
            m[0] = value;
        `;
    for (let i = 1; i < rank - 1; ++i) {
      block += `
        else if (index == ${i})
            m[${i}] = value;
            `;
    }
    block += `
        else
            m[${rank - 1}] = value;
        `;
    const body = `
      void setVecItem(out int m[${rank}], int index, int value) {
        ${block}
      }
        `;
    return { setVecItem: new GlslLibRoutine(body) };
  }
  protected getVecItem(): { [name: string]: GlslLibRoutine } {
    const outputLayout = this.context.outputTextureLayout;
    const rank = outputLayout.shape.length;
    let block = `
        if(index < 0)
            index = ${rank} + index;
        if (index == 0)
            return m[0];
      `;
    for (let i = 1; i < rank - 1; ++i) {
      block += `
        else if (index == ${i})
            return m[${i}];
      `;
    }
    block += `
        else
            return m[${rank - 1}];
        `;
    const body = `
      int getVecItem(int m[${rank}], int index) {
        ${block}
      }
    `;
    return { getVecItem: new GlslLibRoutine(body) };
  }
}
