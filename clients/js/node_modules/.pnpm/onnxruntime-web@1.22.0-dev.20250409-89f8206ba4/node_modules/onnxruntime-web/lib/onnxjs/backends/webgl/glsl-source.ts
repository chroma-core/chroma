// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * represent a version irrelevant abstraction of for GLSL source code
 */
export interface Glsl {
  readonly version: string;
  readonly attribute: string;
  readonly varyingVertex: string;
  readonly varyingFrag: string;
  readonly texture2D: string;
  readonly output: string;
  readonly outputDeclaration: string;
}

const GLSL_ES_2_0: Glsl = {
  version: '',
  attribute: 'attribute',
  varyingVertex: 'varying',
  varyingFrag: 'varying',
  texture2D: 'texture2D',
  output: 'gl_FragColor',
  outputDeclaration: '',
};
const GLSL_ES_3_0: Glsl = {
  version: '#version 300 es',
  attribute: 'in',
  varyingVertex: 'out',
  varyingFrag: 'in',
  texture2D: 'texture',
  output: 'outputColor',
  outputDeclaration: 'out vec4 outputColor;',
};

export function getGlsl(version: 1 | 2) {
  return version === 1 ? GLSL_ES_2_0 : GLSL_ES_3_0;
}

export function getVertexShaderSource(version: 1 | 2): string {
  const glsl = getGlsl(version);
  return `${glsl.version}
      precision highp float;
      ${glsl.attribute} vec3 position;
      ${glsl.attribute} vec2 textureCoord;

      ${glsl.varyingVertex} vec2 TexCoords;

      void main()
      {
          gl_Position = vec4(position, 1.0);
          TexCoords = textureCoord;
      }`;
}

export function getFragShaderPreamble(version: 1 | 2): string {
  const glsl = getGlsl(version);
  return `${glsl.version}
    precision highp float;
    precision highp int;
    precision highp sampler2D;
    ${glsl.varyingFrag} vec2 TexCoords;
    ${glsl.outputDeclaration}
    const vec2 halfCR = vec2(0.5, 0.5);

    // Custom vector types to handle higher dimenalities.
    struct ivec5
    {
      int x;
      int y;
      int z;
      int w;
      int u;
    };

    struct ivec6
    {
      int x;
      int y;
      int z;
      int w;
      int u;
      int v;
    };

    int imod(int x, int y) {
      return x - y * (x / y);
    }

    `;
}

export function getDefaultFragShaderMain(version: 1 | 2, outputShapeLength: number): string {
  const glsl = getGlsl(version);
  return `
  void main() {
    int indices[${outputShapeLength}];
    toVec(TexCoords, indices);
    vec4 result = vec4(process(indices));
    ${glsl.output} = result;
  }
  `;
}
