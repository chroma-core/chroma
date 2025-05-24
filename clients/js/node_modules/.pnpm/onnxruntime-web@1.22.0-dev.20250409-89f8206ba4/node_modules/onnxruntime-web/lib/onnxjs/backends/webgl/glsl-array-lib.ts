// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { GlslContext, GlslLib, GlslLibRoutine } from './glsl-definitions';
/**
 * This library produces routines needed for non-constant access to uniform arrays
 */
export class ArrayGlslLib extends GlslLib {
  getFunctions(): { [name: string]: GlslLibRoutine } {
    return this.generate();
  }
  getCustomTypes(): { [name: string]: string } {
    return {};
  }
  constructor(context: GlslContext) {
    super(context);
  }
  protected generate(): { [name: string]: GlslLibRoutine } {
    const result: { [name: string]: GlslLibRoutine } = {};
    for (let i = 1; i <= 16; i++) {
      result[`setItem${i}`] = new GlslLibRoutine(this.generateSetItem(i));
      result[`getItem${i}`] = new GlslLibRoutine(this.generateGetItem(i));
    }
    return result;
  }
  protected generateSetItem(length: number): string {
    let block = `
       if(index < 0)
           index = ${length} + index;
       if (index == 0)
           a[0] = value;
       `;
    for (let i = 1; i < length - 1; ++i) {
      block += `
       else if (index == ${i})
           a[${i}] = value;
           `;
    }
    block += `
       else
           a[${length - 1}] = value;
       `;
    const body = `
     void setItem${length}(out float a[${length}], int index, float value) {
       ${block}
     }
       `;
    return body;
  }
  protected generateGetItem(length: number): string {
    let block = `
       if(index < 0)
           index = ${length} + index;
       if (index == 0)
           return a[0];
     `;
    for (let i = 1; i < length - 1; ++i) {
      block += `
       else if (index == ${i})
           return a[${i}];
     `;
    }
    block += `
       else
           return a[${length - 1}];
       `;
    const body = `
     float getItem${length}(float a[${length}], int index) {
       ${block}
     }
   `;
    return body;
  }
}
