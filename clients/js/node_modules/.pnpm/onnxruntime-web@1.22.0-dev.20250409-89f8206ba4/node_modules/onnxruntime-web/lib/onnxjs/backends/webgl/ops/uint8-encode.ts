// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { TextureData, TextureType } from '../types';

export const encodeAsUint8 = (inferenceHandler: WebGLInferenceHandler, input: TextureData): TextureData => {
  const outputShape = input.shape;
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  /**
   * https://github.com/tensorflow/tfjs-core/blob/master/src/kernels/webgl/encode_float_gpu.ts
   */
  const shaderSource = `
    const float FLOAT_MAX = 1.70141184e38;
    const float FLOAT_MIN = 1.17549435e-38;

    bool isNaN(float val) {
      return (val < 1.0 || 0.0 < val || val == 0.0) ? false : true;
    }

    highp vec4 encodeAsUint8(highp float v) {
      if (isNaN(v)) {
        return vec4(255, 255, 255, 255);
      }

      highp float av = abs(v);

      if(av < FLOAT_MIN) {
        return vec4(0.0, 0.0, 0.0, 0.0);
      } else if(v > FLOAT_MAX) {
        return vec4(0.0, 0.0, 128.0, 127.0) / 255.0;
      } else if(v < -FLOAT_MAX) {
        return vec4(0.0, 0.0,  128.0, 255.0) / 255.0;
      }

      highp vec4 c = vec4(0,0,0,0);

      highp float e = floor(log2(av));
      highp float m = exp2(fract(log2(av))) - 1.0;

      c[2] = floor(128.0 * m);
      m -= c[2] / 128.0;
      c[1] = floor(32768.0 * m);
      m -= c[1] / 32768.0;
      c[0] = floor(8388608.0 * m);

      highp float ebias = e + 127.0;
      c[3] = floor(ebias / 2.0);
      ebias -= c[3] * 2.0;
      c[2] += floor(ebias) * 128.0;

      c[3] += 128.0 * step(0.0, -v);

      return c / 255.0;
    }

    void main() {
      float value = ${glsl.texture2D}(X,TexCoords).r;
      ${glsl.output} = encodeAsUint8(value);
    }`;
  const programInfo = {
    name: 'Uint8Encode',
    inputTypes: [TextureType.unpacked],
    inputNames: ['X'],
    output: { dims: outputShape, type: input.tensor.type, textureType: TextureType.downloadUint8AsFloat },
    shaderSource,
    hasMain: true,
  };
  return inferenceHandler.executeProgram(programInfo, [input.tensor]);
};
