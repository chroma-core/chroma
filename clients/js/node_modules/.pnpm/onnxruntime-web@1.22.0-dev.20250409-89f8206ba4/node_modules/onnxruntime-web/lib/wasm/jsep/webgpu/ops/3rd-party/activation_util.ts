/**
 * @license
 * Copyright 2021 Google LLC. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =============================================================================
 */

// sampled from [@tensorflow/tfjs] tfjs-backend-webgpu/src/activation_util.ts
//
// modified to fit the needs of the project

export const typeSnippet = (component: number, dataType: string) => {
  switch (component) {
    case 1:
      return dataType;
    case 2:
      return `vec2<${dataType}>`;
    case 3:
      return `vec3<${dataType}>`;
    case 4:
      return `vec4<${dataType}>`;
    default:
      throw new Error(`${component}-component is not supported.`);
  }
};

export const biasSnippet = (hasBias: boolean): string => `
      ${hasBias ? 'value = value + getBiasByOutputCoords(coords);' : ''}
      `;
