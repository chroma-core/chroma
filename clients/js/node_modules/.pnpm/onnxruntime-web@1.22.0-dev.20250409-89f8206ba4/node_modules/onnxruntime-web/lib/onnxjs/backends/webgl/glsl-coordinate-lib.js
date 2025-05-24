'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.CoordsGlslLib = void 0;
const util_1 = require('../../util');
const glsl_definitions_1 = require('./glsl-definitions');
const glsl_source_1 = require('./glsl-source');
const texture_layout_strategy_1 = require('./texture-layout-strategy');
const utils_1 = require('./utils');
/**
 * GLSL Library responsible for data types and routines for manipulating
 * coordinates and mapping to/from tensor indices
 */
class CoordsGlslLib extends glsl_definitions_1.GlslLib {
  constructor(context) {
    super(context);
  }
  getFunctions() {
    return {
      ...this.offsetToCoords(),
      ...this.coordsToOffset(),
      ...this.toVec(),
      ...this.valueFrom(),
      // TODO return these only when packing is enabled.
      ...this.getCommonUtilFuncs(),
      ...this.getInputsSamplingSnippets(),
      ...this.getOutputSamplingSnippet(),
    };
  }
  getCustomTypes() {
    return {};
  }
  /**
   * Produces a function that can map from
   * 2D normalzied coordinates (s,t) to a flat offset
   */
  offsetToCoords() {
    const funcName = 'offsetToCoords';
    return {
      offsetToCoords: new glsl_definitions_1.GlslLibRoutine(`
      vec2 ${funcName}(int offset, int width, int height) {
        int t = offset / width;
        int s = offset - t*width;
        vec2 coords = (vec2(s,t) + vec2(0.5,0.5)) / vec2(width, height);
        return coords;
      }
      `),
    };
  }
  /**
   * Produces a function that can map from
   * 2D normalzied coordinates (s,t) to a flat offset
   */
  coordsToOffset() {
    const funcName = 'coordsToOffset';
    return {
      coordsToOffset: new glsl_definitions_1.GlslLibRoutine(`
      int ${funcName}(vec2 coords, int width, int height) {
        float s = coords.s * float(width);
        float t = coords.t * float(height);
        int offset = int(t) * width + int(s);
        return offset;
      }
      `),
    };
  }
  /**
   * Generates code for output sampler.
   */
  getOutputSamplingSnippet() {
    const outputLayout = this.context.outputTextureLayout;
    if (outputLayout.isPacked) {
      return this.getPackedOutputSamplingSnippet(outputLayout);
    } else {
      return this.getUnpackedOutputSamplingSnippet(outputLayout);
    }
  }
  /**
   * Generates code for packed output sampler.
   */
  getPackedOutputSamplingSnippet(outputLayout) {
    const outShape = outputLayout.unpackedShape;
    const outTexShape = [outputLayout.width, outputLayout.height];
    const result = {};
    const funcName = 'getOutputCoords';
    switch (outShape.length) {
      case 0:
        result[funcName] = this.getOutputScalarCoords();
        break;
      case 1:
        result[funcName] = this.getOutputPacked1DCoords(outShape, outTexShape);
        break;
      case 2:
        result[funcName] = this.getOutputPacked2DCoords(outShape, outTexShape);
        break;
      case 3:
        result[funcName] = this.getOutputPacked3DCoords(outShape, outTexShape);
        break;
      default:
        result[funcName] = this.getOutputPackedNDCoords(outShape, outTexShape);
    }
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    // TODO we need this to properly return a packed vec4 from kernels.
    // Replace all '{glsl.output} = result' with 'setOutput(result)' in all kernels.
    const floatTextureSetRGBASource = `
      void setOutput(vec4 val) {
        ${glsl.output} = val;
      }
    `;
    const floatTextureSetRGBAFuncName = 'floatTextureSetRGBA';
    result[floatTextureSetRGBAFuncName] = new glsl_definitions_1.GlslLibRoutine(floatTextureSetRGBASource);
    return result;
  }
  /**
   * Generates code for unpacked output sampler.
   */
  getUnpackedOutputSamplingSnippet(outputLayout) {
    const outShape = outputLayout.unpackedShape;
    const outTexShape = [outputLayout.width, outputLayout.height];
    const result = {};
    const funcName = 'getOutputCoords';
    switch (outShape.length) {
      case 0:
        result[funcName] = this.getOutputScalarCoords();
        break;
      case 1:
        result[funcName] = this.getOutputUnpacked1DCoords(outShape, outTexShape);
        break;
      case 2:
        result[funcName] = this.getOutputUnpacked2DCoords(outShape, outTexShape);
        break;
      case 3:
        result[funcName] = this.getOutputUnpacked3DCoords(outShape, outTexShape);
        break;
      case 4:
        result[funcName] = this.getOutputUnpacked4DCoords(outShape, outTexShape);
        break;
      case 5:
        result[funcName] = this.getOutputUnpacked5DCoords(outShape, outTexShape);
        break;
      case 6:
        result[funcName] = this.getOutputUnpacked6DCoords(outShape, outTexShape);
        break;
      default:
        throw new Error(`Unsupported output dimensionality: ${outShape.length}`);
    }
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    // TODO we need this to properly return a packed vec4 from kernels.
    // Replace all '{glsl.output} = result' with 'setOutput(result)' in all kernels.
    const floatTextureSetRSource = `
        void setOutput(float val) {
          ${glsl.output} = vec4(val, 0, 0, 0);
        }
    `;
    const floatTextureSetRFuncName = 'floatTextureSetR';
    result[floatTextureSetRFuncName] = new glsl_definitions_1.GlslLibRoutine(floatTextureSetRSource);
    return result;
  }
  /**
   * Scalar output coordinates.
   */
  getOutputScalarCoords() {
    return new glsl_definitions_1.GlslLibRoutine(`
      int getOutputCoords() {
        return 0;
      }
    `);
  }
  /**
   * 1D packed output coordinates.
   */
  getOutputPacked1DCoords(_shape, texShape) {
    const packedTexShape = texShape;
    let source = '';
    if (packedTexShape[0] === 1) {
      source = `
          int getOutputCoords() {
            return 2 * int(TexCoords.y * ${packedTexShape[1]}.0);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source);
    }
    if (packedTexShape[1] === 1) {
      source = `
          int getOutputCoords() {
            return 2 * int(TexCoords.x * ${packedTexShape[0]}.0);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source);
    }
    source = `
        int getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                 vec2(${packedTexShape[0]}, ${packedTexShape[1]}));
          return 2 * (resTexRC.y * ${packedTexShape[0]} + resTexRC.x);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * 2D packed output coordinates.
   */
  getOutputPacked2DCoords(shape, texShape) {
    let source = '';
    if (util_1.ArrayUtil.arraysEqual(shape, texShape)) {
      source = `
        ivec2 getOutputCoords() {
          return 2 * ivec2(TexCoords.xy * vec2(${texShape[0]}, ${texShape[1]}));
        }
      `;
      return new glsl_definitions_1.GlslLibRoutine(source);
    }
    const packedTexShape = texShape;
    // texels needed to accommodate a logical row
    const texelsInLogicalRow = Math.ceil(shape[1] / 2);
    /**
     * getOutputCoords
     *
     * resTexRC: The rows and columns of the texels. If you move over one
     * texel to the right in the packed texture, you are moving over one column
     * (not two).
     *
     * index: The texel index
     */
    source = `
        ivec2 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${packedTexShape[0]}, ${packedTexShape[1]}));

          int index = resTexRC.y * ${packedTexShape[0]} + resTexRC.x;

          // reverse r and c order for packed texture
          int r = imod(index, ${texelsInLogicalRow}) * 2;
          int c = 2 * (index / ${texelsInLogicalRow});

          return ivec2(r, c);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * 3D packed output coordinates.
   */
  getOutputPacked3DCoords(shape, texShape) {
    const packedTexShape = [texShape[0], texShape[1]];
    const texelsInLogicalRow = Math.ceil(shape[2] / 2);
    const texelsInBatch = texelsInLogicalRow * Math.ceil(shape[1] / 2);
    const source = `
        ivec3 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${packedTexShape[0]}, ${packedTexShape[1]}));
          int index = resTexRC.y * ${packedTexShape[0]} + resTexRC.x;

          int b = index / ${texelsInBatch};
          index -= b * ${texelsInBatch};

          // reverse r and c order for packed texture
          int r = imod(index, ${texelsInLogicalRow}) * 2;
          int c = 2 * (index / ${texelsInLogicalRow});

          return ivec3(b, r, c);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * ND packed output coordinates.
   */
  getOutputPackedNDCoords(shape, texShape) {
    const packedTexShape = [texShape[0], texShape[1]];
    const texelsInLogicalRow = Math.ceil(shape[shape.length - 1] / 2);
    const texelsInBatch = texelsInLogicalRow * Math.ceil(shape[shape.length - 2] / 2);
    let texelsInBatchN = texelsInBatch;
    let batches = '';
    let coords = 'b, r, c';
    for (let b = 2; b < shape.length - 1; b++) {
      texelsInBatchN *= shape[shape.length - b - 1];
      batches =
        `
      int b${b} = index / ${texelsInBatchN};
      index -= b${b} * ${texelsInBatchN};
    ` + batches;
      coords = `b${b}, ` + coords;
    }
    const source = `
      ivec${shape.length} getOutputCoords() {
        ivec2 resTexRC = ivec2(TexCoords.xy *
                              vec2(${packedTexShape[0]}, ${packedTexShape[1]}));
        int index = resTexRC.y * ${packedTexShape[0]} + resTexRC.x;

        ${batches}

        int b = index / ${texelsInBatch};
        index -= b * ${texelsInBatch};

        // reverse r and c order for packed texture
        int r = imod(index, ${texelsInLogicalRow}) * 2;
        int c = 2 * (index / ${texelsInLogicalRow});

        return ivec${shape.length}(${coords});
      }
    `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 1D output coordinates.
   */
  getOutputUnpacked1DCoords(_shape, texShape) {
    const source = `
        int getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${texShape[0]}, ${texShape[1]}));
          return resTexRC.y * ${texShape[0]} + resTexRC.x;
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 2D output coordinates.
   */
  getOutputUnpacked2DCoords(shape, texShape) {
    const source = `
        ivec2 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${texShape[0]}, ${texShape[1]}));
          int index = resTexRC.y * ${texShape[0]} + resTexRC.x;
          int r = index / ${shape[1]};
          int c = index - r * ${shape[1]};
          return ivec2(r, c);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 3D output coordinates.
   */
  getOutputUnpacked3DCoords(shape, texShape) {
    let source = '';
    const rank = shape.length;
    let strides = null;
    if (rank < 2) {
      strides = [];
    }
    strides = new Array(rank - 1);
    strides[rank - 2] = shape[rank - 1];
    for (let i = rank - 3; i >= 0; --i) {
      strides[i] = strides[i + 1] * shape[i + 1];
    }
    const coordsToCompute = ['r', 'c', 'd'];
    const coordsFromIndexSnippet = strides
      .map((stride, i) => {
        const line1 = `int ${coordsToCompute[i]} = index / ${stride}`;
        const line2 =
          i === strides.length - 1
            ? `int ${coordsToCompute[i + 1]} = index - ${coordsToCompute[i]} * ${stride}`
            : `index -= ${coordsToCompute[i]} * ${stride}`;
        return `${line1}; ${line2};`;
      })
      .join('');
    source = `
        ivec3 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${texShape[0]}, ${texShape[1]}));
          int index = resTexRC.y * ${texShape[0]} + resTexRC.x;
          ${coordsFromIndexSnippet}
          return ivec3(r, c, d);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 4D output coordinates.
   */
  getOutputUnpacked4DCoords(shape, texShape) {
    let source = '';
    const rank = shape.length;
    let strides = null;
    if (rank < 2) {
      strides = [];
    }
    strides = new Array(rank - 1);
    strides[rank - 2] = shape[rank - 1];
    for (let i = rank - 3; i >= 0; --i) {
      strides[i] = strides[i + 1] * shape[i + 1];
    }
    const coordsToCompute = ['r', 'c', 'd', 'd2'];
    const coordsFromIndexSnippet = strides
      .map((stride, i) => {
        const line1 = `int ${coordsToCompute[i]} = index / ${stride}`;
        const line2 =
          i === strides.length - 1
            ? `int ${coordsToCompute[i + 1]} = index - ${coordsToCompute[i]} * ${stride}`
            : `index -= ${coordsToCompute[i]} * ${stride}`;
        return `${line1}; ${line2};`;
      })
      .join('');
    source = `
      ivec4 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${texShape[0]}, ${texShape[1]}));
          int index = resTexRC.y * ${texShape[0]} + resTexRC.x;
          ${coordsFromIndexSnippet}
          return ivec4(r, c, d, d2);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 5D output coordinates.
   */
  getOutputUnpacked5DCoords(shape, texShape) {
    let source = '';
    const rank = shape.length;
    let strides = null;
    if (rank < 2) {
      strides = [];
    }
    strides = new Array(rank - 1);
    strides[rank - 2] = shape[rank - 1];
    for (let i = rank - 3; i >= 0; --i) {
      strides[i] = strides[i + 1] * shape[i + 1];
    }
    const coordsToCompute = ['r', 'c', 'd', 'd2', 'd3'];
    const coordsFromIndexSnippet = strides
      .map((stride, i) => {
        const line1 = `int ${coordsToCompute[i]} = index / ${stride}`;
        const line2 =
          i === strides.length - 1
            ? `int ${coordsToCompute[i + 1]} = index - ${coordsToCompute[i]} * ${stride}`
            : `index -= ${coordsToCompute[i]} * ${stride}`;
        return `${line1}; ${line2};`;
      })
      .join('');
    source = `
      ivec5 getOutputCoords() {
          ivec2 resTexRC = ivec2(TexCoords.xy *
                                vec2(${texShape[0]}, ${texShape[1]}));
          int index = resTexRC.y * ${texShape[0]} + resTexRC.x;
          ${coordsFromIndexSnippet}
          return ivec5(r, c, d, d2, d3);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked 6D output coordinates.
   */
  getOutputUnpacked6DCoords(shape, texShape) {
    let source = '';
    const rank = shape.length;
    let strides = null;
    if (rank < 2) {
      strides = [];
    }
    strides = new Array(rank - 1);
    strides[rank - 2] = shape[rank - 1];
    for (let i = rank - 3; i >= 0; --i) {
      strides[i] = strides[i + 1] * shape[i + 1];
    }
    const coordsToCompute = ['r', 'c', 'd', 'd2', 'd3', 'd4'];
    const coordsFromIndexSnippet = strides
      .map((stride, i) => {
        const line1 = `int ${coordsToCompute[i]} = index / ${stride}`;
        const line2 =
          i === strides.length - 1
            ? `int ${coordsToCompute[i + 1]} = index - ${coordsToCompute[i]} * ${stride}`
            : `index -= ${coordsToCompute[i]} * ${stride}`;
        return `${line1}; ${line2};`;
      })
      .join('');
    source = `
     ivec6 getOutputCoords() {
         ivec2 resTexRC = ivec2(TexCoords.xy *
                               vec2(${texShape[0]}, ${texShape[1]}));
         int index = resTexRC.y * ${texShape[0]} + resTexRC.x;
         ${coordsFromIndexSnippet}
         return ivec6(r, c, d, d2, d3, d4);
       }
     `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Generates code for common UV coords computation utility functions.
   */
  getCommonUtilFuncs() {
    const result = {};
    let funcName = 'uvFromFlat';
    result[funcName] = new glsl_definitions_1.GlslLibRoutine(`
    vec2 uvFromFlat(int texNumR, int texNumC, int index) {
      int texC = index / texNumR;
      int texR = index - texC * texNumR;
      // TODO: swap texR, texC order in following function so row is corresponding to u and column is corresponding to
      //       v.
      return (vec2(texR, texC) + halfCR) / vec2(texNumR, texNumC);
    }
    `);
    funcName = 'packedUVfrom1D';
    result[funcName] = new glsl_definitions_1.GlslLibRoutine(`
      vec2 packedUVfrom1D(int texNumR, int texNumC, int index) {
        int texelIndex = index / 2;
        int texR = texelIndex / texNumC;
        int texC = texelIndex - texR * texNumC;
        return (vec2(texC, texR) + halfCR) / vec2(texNumC, texNumR);
      }
      `);
    funcName = 'packedUVfrom2D';
    result[funcName] = new glsl_definitions_1.GlslLibRoutine(`
      vec2 packedUVfrom2D(int texNumR, int texNumC, int texelsInLogicalRow, int row, int col) {
        int texelIndex = (row / 2) * texelsInLogicalRow + (col / 2);
        int texR = texelIndex / texNumC;
        int texC = texelIndex - texR * texNumC;
        return (vec2(texC, texR) + halfCR) / vec2(texNumC, texNumR);
      }
      `);
    funcName = 'packedUVfrom3D';
    result[funcName] = new glsl_definitions_1.GlslLibRoutine(`
      vec2 packedUVfrom3D(int texNumR, int texNumC,
          int texelsInBatch, int texelsInLogicalRow, int b,
          int row, int col) {
        int index = b * texelsInBatch + (row / 2) * texelsInLogicalRow + (col / 2);
        int texR = index / texNumC;
        int texC = index - texR * texNumC;
        return (vec2(texC, texR) + halfCR) / vec2(texNumC, texNumR);
      }
      `);
    funcName = 'sampleTexture';
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    result[funcName] = new glsl_definitions_1.GlslLibRoutine(`
        float sampleTexture(sampler2D textureSampler, vec2 uv) {
            return ${glsl.texture2D}(textureSampler, uv).r;
        }`);
    return result;
  }
  /**
   * Constructing snippets for inputs
   */
  getInputsSamplingSnippets() {
    const result = {};
    const outputLayout = this.context.outputTextureLayout;
    this.context.programInfo.inputNames.forEach((samplerName, i) => {
      const inputLayout = this.context.inputTextureLayouts[i];
      const funcName = (0, utils_1.generateShaderFuncNameFromInputSamplerName)(samplerName);
      if (inputLayout.isPacked) {
        result[funcName] = this.getPackedSamplerFromInput(funcName, samplerName, inputLayout);
      } else {
        result[funcName] = this.getUnpackedSamplerFromInput(funcName, samplerName, inputLayout);
      }
      const outCoordFuncName = (0, utils_1.generateShaderFuncNameFromInputSamplerNameAtOutCoords)(samplerName);
      if (inputLayout.unpackedShape.length <= outputLayout.unpackedShape.length) {
        if (inputLayout.isPacked) {
          result[outCoordFuncName] = this.getPackedSamplerAtOutputCoords(
            outCoordFuncName,
            inputLayout,
            outputLayout,
            samplerName,
          );
        } else {
          result[outCoordFuncName] = this.getUnpackedSamplerAtOutputCoords(
            outCoordFuncName,
            inputLayout,
            outputLayout,
            samplerName,
          );
        }
      }
    });
    return result;
  }
  /**
   * Constructing snippets for output coordinates of samplers
   */
  getPackedSamplerAtOutputCoords(funcName, inputLayout, outputLayout, name) {
    const inShape = inputLayout.unpackedShape;
    const outShape = outputLayout.unpackedShape;
    const texName = name;
    const texFuncSnippet = (0, utils_1.generateShaderFuncNameFromInputSamplerName)(texName);
    const inRank = inShape.length;
    const outRank = outShape.length;
    const broadcastDims = util_1.BroadcastUtil.getBroadcastDims(inShape, outShape);
    const type = (0, utils_1.getCoordsDataType)(outRank);
    const rankDiff = outRank - inRank;
    let coordsSnippet;
    const fields = (0, utils_1.getGlChannels)();
    if (inRank === 0) {
      coordsSnippet = '';
    } else if (outRank < 2 && broadcastDims.length >= 1) {
      coordsSnippet = 'coords = 0;';
    } else {
      coordsSnippet = broadcastDims.map((d) => `coords.${fields[d + rankDiff]} = 0;`).join('\n');
    }
    let unpackedCoordsSnippet = '';
    if (outRank < 2 && inRank > 0) {
      unpackedCoordsSnippet = 'coords';
    } else {
      unpackedCoordsSnippet = inShape.map((_s, i) => `coords.${fields[i + rankDiff]}`).join(', ');
    }
    let output = 'return outputValue;';
    const inSize = util_1.ShapeUtil.size(inShape);
    const isInputScalar = inSize === 1;
    const outSize = util_1.ShapeUtil.size(outShape);
    const isOutputScalar = outSize === 1;
    if (inRank === 1 && !isInputScalar && !isOutputScalar) {
      output = `
        return vec4(outputValue.xy, outputValue.xy);
      `;
    } else if (isInputScalar && !isOutputScalar) {
      if (outRank === 1) {
        output = `
          return vec4(outputValue.x, outputValue.x, 0., 0.);
        `;
      } else {
        output = `
          return vec4(outputValue.x);
        `;
      }
    } else if (broadcastDims.length) {
      const rows = inRank - 2;
      const cols = inRank - 1;
      if (broadcastDims.indexOf(rows) > -1 && broadcastDims.indexOf(cols) > -1) {
        output = 'return vec4(outputValue.x);';
      } else if (broadcastDims.indexOf(rows) > -1) {
        output = 'return vec4(outputValue.x, outputValue.y, ' + 'outputValue.x, outputValue.y);';
      } else if (broadcastDims.indexOf(cols) > -1) {
        output = 'return vec4(outputValue.xx, outputValue.zz);';
      }
    }
    const swapLastDimsSnippet = `
        int lastDim = coords.${fields[outRank - 1]};
        coords.${fields[outRank - 1]} = coords.${fields[outRank - 2]};
        coords.${fields[outRank - 2]} = lastDim;
      `;
    const source = `
      vec4 ${funcName}() {
        ${type} coords = getOutputCoords();
        ${swapLastDimsSnippet}
        ${coordsSnippet}
        vec4 outputValue = ${texFuncSnippet}(${unpackedCoordsSnippet});
        ${output}
      }
    `;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.getOutputCoords']);
  }
  /**
   * Constructing snippets for unpacked output coordinates of samplers
   */
  getUnpackedSamplerAtOutputCoords(funcName, inputLayout, outputLayout, name) {
    const outTexShape = [outputLayout.width, outputLayout.height];
    const inTexShape = [inputLayout.width, inputLayout.height];
    const inRank = inputLayout.unpackedShape.length;
    const outRank = outputLayout.unpackedShape.length;
    const inShape = inputLayout.unpackedShape;
    const outShape = outputLayout.unpackedShape;
    const texFuncSnippet = (0, utils_1.generateShaderFuncNameFromInputSamplerName)(name);
    if (inRank === outRank && util_1.ArrayUtil.arraysEqual(inTexShape, outTexShape)) {
      const source = `
          float ${funcName}() {
            return sampleTexture(${name}, TexCoords);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    const type = (0, utils_1.getCoordsDataType)(outRank);
    const broadcastDims = util_1.BroadcastUtil.getBroadcastDims(inShape, outShape);
    const rankDiff = outRank - inRank;
    let coordsSnippet;
    const fields = (0, utils_1.getGlChannels)();
    if (inRank === 0) {
      coordsSnippet = '';
    } else if (outRank < 2 && broadcastDims.length >= 1) {
      coordsSnippet = 'coords = 0;';
    } else {
      coordsSnippet = broadcastDims.map((d) => `coords.${fields[d + rankDiff]} = 0;`).join('\n');
    }
    let unpackedCoordsSnippet = '';
    if (outRank < 2 && inRank > 0) {
      unpackedCoordsSnippet = 'coords';
    } else {
      unpackedCoordsSnippet = inputLayout.unpackedShape.map((_s, i) => `coords.${fields[i + rankDiff]}`).join(', ');
    }
    const source = `
        float ${funcName}() {
          ${type} coords = getOutputCoords();
          ${coordsSnippet}
          return ${texFuncSnippet}(${unpackedCoordsSnippet});
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.getOutputCoords']);
  }
  /**
   * Constructing snippets for packed operations.
   */
  getPackedSamplerFromInput(funcName, name, inputLayout) {
    switch (inputLayout.unpackedShape.length) {
      case 0:
        return this.getPackedSamplerScalar(funcName, name);
      case 1:
        return this.getPackedSampler1D(funcName, name, inputLayout);
      case 2:
        return this.getPackedSampler2D(funcName, name, inputLayout);
      case 3:
        return this.getPackedSampler3D(funcName, name, inputLayout);
      default:
        return this.getPackedSamplerND(funcName, name, inputLayout);
    }
  }
  /**
   * Constructing snippets for unpacked operations.
   */
  getUnpackedSamplerFromInput(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    switch (shape.length) {
      case 0:
        return this.getUnpackedSamplerScalar(funcName, name, inputLayout);
      case 1:
        return this.getUnpackedSampler1D(funcName, name, inputLayout);
      case 2:
        return this.getUnpackedSampler2D(funcName, name, inputLayout);
      case 3:
        return this.getUnpackedSampler3D(funcName, name, inputLayout);
      case 4:
        return this.getUnpackedSampler4D(funcName, name, inputLayout);
      case 5:
        return this.getUnpackedSampler5D(funcName, name, inputLayout);
      case 6:
        return this.getUnpackedSampler6D(funcName, name, inputLayout);
      default:
        // TODO support more dimensionalities
        throw new Error(`Unsupported dimension ${shape.length}-D`);
    }
  }
  /**
   * Packed scalar snippet.
   */
  getPackedSamplerScalar(funcName, name) {
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    const source = `
          vec4 ${funcName}() {
            return ${glsl.texture2D}(${name}, halfCR);
          }
        `;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Packed 1D snippet.
   */
  getPackedSampler1D(funcName, name, inputLayout) {
    const texShape = [inputLayout.width, inputLayout.height];
    const packedTexShape = [texShape[1], texShape[0]];
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    const packedSampler = `vec4 ${funcName}(int index) {
      vec2 uv = packedUVfrom1D(
      ${packedTexShape[0]}, ${packedTexShape[1]}, index);
      return ${glsl.texture2D}(${name}, uv);
    }`;
    const source = packedSampler;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.packedUVfrom1D']);
  }
  /**
   * Packed 2D snippet.
   */
  getPackedSampler2D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const texShape = [inputLayout.width, inputLayout.height];
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    const texNumR = texShape[0];
    const texNumC = texShape[1];
    if (texShape != null && util_1.ArrayUtil.arraysEqual(shape, texShape)) {
      const packedSampler = `vec4 ${funcName}(int row, int col) {
        vec2 uv = (vec2(col, row) + halfCR) / vec2(${texNumC}.0, ${texNumR}.0);
        return ${glsl.texture2D}(${name}, uv);
      }`;
      return new glsl_definitions_1.GlslLibRoutine(packedSampler);
    }
    const packedTexShape = texShape;
    const valuesPerRow = Math.ceil(shape[1] / 2);
    const packedSampler = `vec4 ${funcName}(int row, int col) {
      vec2 uv = packedUVfrom2D(${packedTexShape[1]}, ${packedTexShape[0]}, ${valuesPerRow}, row, col);
      return ${glsl.texture2D}(${name}, uv);
    }`;
    const source = packedSampler;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.packedUVfrom2D']);
  }
  /**
   * Packed 3D snippet.
   */
  getPackedSampler3D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const texShape = [inputLayout.width, inputLayout.height];
    const packedTexShape = [texShape[0], texShape[1]];
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    if (shape[0] === 1) {
      const squeezedShape = shape.slice(1);
      const keptDims = [1, 2];
      const newInputShape = (0, utils_1.squeezeInputShape)(shape, squeezedShape);
      const params = ['b', 'row', 'col'];
      // Deep copy of input texture layout.
      const newInputLayout = JSON.parse(JSON.stringify(inputLayout));
      newInputLayout.unpackedShape = newInputShape;
      const samplerRoutine = this.getPackedSamplerFromInput(funcName, name, newInputLayout);
      const packedSampler = `${samplerRoutine.routineBody}
      vec4 ${funcName}(int b, int row, int col) {
        return ${funcName}(${(0, utils_1.getSqueezedParams)(params, keptDims)});
      } `;
      const source = packedSampler;
      return new glsl_definitions_1.GlslLibRoutine(source, samplerRoutine.dependencies);
    }
    const texNumR = packedTexShape[0];
    const texNumC = packedTexShape[1];
    const valuesPerRow = Math.ceil(shape[2] / 2);
    const texelsInBatch = valuesPerRow * Math.ceil(shape[1] / 2);
    const packedSampler = `vec4 ${funcName}(int b, int row, int col) {
      vec2 uv = packedUVfrom3D(
        ${texNumC}, ${texNumR}, ${texelsInBatch}, ${valuesPerRow}, b, row, col);
      return ${glsl.texture2D}(${name}, uv);}`;
    const source = packedSampler;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.packedUVfrom3D']);
  }
  /*
   * Packed ND snippet.
   */
  getPackedSamplerND(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const rank = shape.length;
    const texShape = [inputLayout.width, inputLayout.height];
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    const packedTexShape = [texShape[0], texShape[1]];
    const texNumR = packedTexShape[1];
    const texNumC = packedTexShape[0];
    const valuesPerRow = Math.ceil(shape[rank - 1] / 2);
    let texelsInBatch = valuesPerRow * Math.ceil(shape[rank - 2] / 2);
    let params = 'int b, int row, int col';
    let index = `b * ${texelsInBatch} + (row / 2) * ${valuesPerRow} + (col / 2)`;
    for (let b = 2; b < rank - 1; b++) {
      params = `int b${b}, ` + params;
      texelsInBatch *= shape[rank - b - 1];
      index = `b${b} * ${texelsInBatch} + ` + index;
    }
    const packedSampler = `vec4 ${funcName}(${params}) {
      int index = ${index};
      int texR = index / ${texNumC};
      int texC = index - texR * ${texNumC};
      vec2 uv = (vec2(texC, texR) + halfCR) / vec2(${texNumC}, ${texNumR});
      return ${glsl.texture2D}(${name}, uv);
    }`;
    const source = packedSampler;
    return new glsl_definitions_1.GlslLibRoutine(source);
  }
  /**
   * Unpacked scalar snippet.
   */
  getUnpackedSamplerScalar(funcName, name, inputLayout) {
    const [texNumR, texNumC] = [inputLayout.width, inputLayout.height];
    if (texNumR === 1 && texNumC === 1) {
      const source = `
          float ${funcName}() {
            return sampleTexture(${name}, halfCR);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    const source = `
        float ${funcName}() {
          int offset_${name} = coordsToOffset(TexCoords, ${texNumR}, ${texNumC});
          vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, offset_${name});
          return sampleTexture(${name}, uv);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, [
      'coordinates.uvFromFlat',
      'coordinates.sampleTexture',
      'coordinates.coordsToOffset',
    ]);
  }
  /**
   * Unpacked 1D snippet.
   */
  getUnpackedSampler1D(funcName, name, inputLayout) {
    const tNumR = inputLayout.width;
    const tNumC = inputLayout.height;
    if (tNumC === 1 && tNumR === 1) {
      const source = `
        float ${funcName}(int index) {
          return sampleTexture(${name}, halfCR);
        }
      `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    if (tNumC === 1) {
      const source = `
          float ${funcName}(int index) {
            vec2 uv = vec2((float(index) + 0.5) / ${tNumR}.0, 0.5);
            return sampleTexture(${name}, uv);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    if (tNumR === 1) {
      const source = `
          float ${funcName}(int index) {
            vec2 uv = vec2(0.5, (float(index) + 0.5) / ${tNumC}.0);
            return sampleTexture(${name}, uv);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    const source = `
        float ${funcName}(int index) {
          vec2 uv = uvFromFlat(${tNumR}, ${tNumC}, index);
          return sampleTexture(${name}, uv);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.uvFromFlat', 'coordinates.sampleTexture']);
  }
  /**
   * Unpacked 2D snippet.
   */
  getUnpackedSampler2D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    // TODO: modify row/col order for other dimensions.
    const texShape = [inputLayout.height, inputLayout.width];
    if (texShape != null && util_1.ArrayUtil.arraysEqual(shape, texShape)) {
      const texNumR = texShape[1];
      const texNumC = texShape[0];
      const source = `
          float ${funcName}(int row, int col) {
            vec2 uv = (vec2(row, col) + halfCR) / vec2(${texNumR}.0, ${texNumC}.0);
            return sampleTexture(${name}, uv);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    const { newShape, keptDims } = (0, texture_layout_strategy_1.squeezeShape)(shape);
    const squeezedShape = newShape;
    if (squeezedShape.length < shape.length) {
      const newInputShape = (0, utils_1.squeezeInputShape)(shape, squeezedShape);
      // Deep copy of input texture layout.
      const newInputLayout = JSON.parse(JSON.stringify(inputLayout));
      newInputLayout.unpackedShape = newInputShape;
      const params = ['col', 'row'];
      const source = `
          ${this.getUnpackedSamplerFromInput(funcName, name, newInputLayout).routineBody}
          float ${funcName}(int row, int col) {
            return ${funcName}(${(0, utils_1.getSqueezedParams)(params, keptDims)});
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture']);
    }
    const texNumR = texShape[1];
    const texNumC = texShape[0];
    if (texNumC === 1) {
      const source = `
          float ${funcName}(int row, int col) {
            int offset_${name} = coordsToOffset(TexCoords, ${texNumR}, ${texNumC});
            float index = dot(vec3(row, col, offset_${name}), vec3(${shape[1]}, 1, 1));
            vec2 uv = vec2(0.5, (index + 0.5) / ${texNumR}.0);
            return sampleTexture(${name}, uv);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture', 'coordinates.coordsToOffset']);
    }
    if (texNumR === 1) {
      const source = `
          float ${funcName}(int row, int col) {
            int offset_${name} = coordsToOffset(TexCoords, ${texNumR}, ${texNumC});
            float index = dot(vec3(row, col, offset_${name}), vec3(${shape[1]}, 1, 1));
            vec2 uv = vec2((index + 0.5) / ${texNumC}.0, 0.5);
            return sampleTexture(${name}, uv);
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture', 'coordinates.coordsToOffset']);
    }
    const source = `
        float ${funcName}(int row, int col) {
          int index = col * ${shape[1]} + row;
          vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, index);
          return sampleTexture(${name}, uv);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, [
      'coordinates.uvFromFlat',
      'coordinates.sampleTexture',
      'coordinates.coordsToOffset',
    ]);
  }
  /**
   * Unpacked 3D snippet.
   */
  getUnpackedSampler3D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const stride0 = shape[1] * shape[2];
    const stride1 = shape[2];
    const { newShape, keptDims } = (0, texture_layout_strategy_1.squeezeShape)(shape);
    const squeezedShape = newShape;
    if (squeezedShape.length < shape.length) {
      const newInputShape = (0, utils_1.squeezeInputShape)(shape, squeezedShape);
      const params = ['batch', 'col', 'row'];
      // Deep copy of input texture layout.
      const newInputLayout = JSON.parse(JSON.stringify(inputLayout));
      newInputLayout.unpackedShape = newInputShape;
      const routine = this.getUnpackedSamplerFromInput(funcName, name, newInputLayout);
      // TODO: revisit the logic here to make it simpler
      const revDims = keptDims.reverse();
      const source = `
          ${routine.routineBody}
          float ${funcName}(int batch, int row, int col) {
            return ${funcName}(${(0, utils_1.getSqueezedParams)(params, revDims)});
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, routine.dependencies);
    }
    const texNumR = inputLayout.width;
    const texNumC = inputLayout.height;
    const source = `
          float ${funcName}(int depth, int row, int col) {
            // Explicitly use integer operations as dot() only works on floats.
            int index = depth * ${stride0} + col * ${stride1} + row;
            vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, index);
            return sampleTexture(${name}, uv);
          }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, [
      'coordinates.uvFromFlat',
      'coordinates.sampleTexture',
      'coordinates.coordsToOffset',
    ]);
  }
  /**
   * Unpacked 4D snippet.
   */
  getUnpackedSampler4D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const stride2 = shape[3];
    const stride1 = shape[2] * stride2;
    const stride0 = shape[1] * stride1;
    //
    // TODO: re-enable this shortcut once the index calculation bug is fixed.
    //
    // const {newShape, keptDims} = squeezeShape(shape as number[]);
    // if (newShape.length < shape.length) {
    //   const newInputShape = squeezeInputShape(shape, newShape);
    //   const params = ['row', 'col', 'depth', 'depth2'];
    //   // Deep copy of input texture layout.
    //   const newInputLayout: TextureLayout = JSON.parse(JSON.stringify(inputLayout));
    //   newInputLayout.unpackedShape = newInputShape;
    //   const source = `
    //       ${this.getUnpackedSamplerFromInput(funcName, name, newInputLayout).routineBody}
    //       float ${funcName}(int row, int col, int depth, int depth2) {
    //         return ${funcName}(${getSqueezedParams(params, keptDims)});
    //       }
    //     `;
    //   return new GlslLibRoutine(
    //       source, ['coordinates.uvFromFlat', 'coordinates.sampleTexture', 'coordinates.coordsToOffset']);
    // }
    const texNumR = inputLayout.width;
    const texNumC = inputLayout.height;
    const source = `
        float ${funcName}(int row, int col, int depth, int depth2) {
          int index = row * ${stride0} + col * ${stride1} +
              depth2 * ${stride2} + depth;
          vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, index);
          return sampleTexture(${name}, uv);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.uvFromFlat', 'coordinates.sampleTexture']);
  }
  /**
   * Unpacked 5D snippet.
   */
  getUnpackedSampler5D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const stride3 = shape[4];
    const stride2 = shape[3] * stride3;
    const stride1 = shape[2] * stride2;
    const stride0 = shape[1] * stride1;
    const { newShape, keptDims } = (0, texture_layout_strategy_1.squeezeShape)(shape);
    if (newShape.length < shape.length) {
      const newInputShape = (0, utils_1.squeezeInputShape)(shape, newShape);
      const params = ['row', 'col', 'depth', 'depth2', 'depth3'];
      // Deep copy of input texture layout.
      const newInputLayout = JSON.parse(JSON.stringify(inputLayout));
      newInputLayout.unpackedShape = newInputShape;
      const source = `
          ${this.getUnpackedSamplerFromInput(funcName, name, newInputLayout).routineBody}
          float ${funcName}(int row, int col, int depth, int depth2, int depth3) {
            return ${funcName}(${(0, utils_1.getSqueezedParams)(params, keptDims)});
          }
        `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture', 'coordinates.uvFromFlat']);
    }
    const texNumR = inputLayout.width;
    const texNumC = inputLayout.height;
    const source = `
        float ${funcName}(int row, int col, int depth, int depth2, int depth3) {
          int index = row * ${stride0} + col * ${stride1} + depth * ${stride2} +
          depth3 * ${stride3} + depth2;
          vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, index);
          return sampleTexture(${name}, uv);
        }
      `;
    return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture', 'coordinates.uvFromFlat']);
  }
  /**
   * Unpacked 6D snippet.
   */
  getUnpackedSampler6D(funcName, name, inputLayout) {
    const shape = inputLayout.unpackedShape;
    const stride4 = shape[5];
    const stride3 = shape[4] * stride4;
    const stride2 = shape[3] * stride3;
    const stride1 = shape[2] * stride2;
    const stride0 = shape[1] * stride1;
    const { newShape, keptDims } = (0, texture_layout_strategy_1.squeezeShape)(shape);
    if (newShape.length < shape.length) {
      const newInputShape = (0, utils_1.squeezeInputShape)(shape, newShape);
      const params = ['row', 'col', 'depth', 'depth2', 'depth3', 'depth4'];
      // Deep copy of input texture layout.
      const newInputLayout = JSON.parse(JSON.stringify(inputLayout));
      newInputLayout.unpackedShape = newInputShape;
      const source = `
            ${this.getUnpackedSamplerFromInput(funcName, name, newInputLayout).routineBody}
            float ${funcName}(int row, int col, int depth,
              int depth2, int depth3, int depth4) {
              return ${funcName}(${(0, utils_1.getSqueezedParams)(params, keptDims)});
            }
          `;
      return new glsl_definitions_1.GlslLibRoutine(source, ['coordinates.sampleTexture', 'coordinates.uvFromFlat']);
    }
    const texNumR = inputLayout.width;
    const texNumC = inputLayout.height;
    const source = `
          float ${funcName}(int row, int col, int depth,
            int depth2, int depth3, int depth4) {
            int index = row * ${stride0} + col * ${stride1} + depth * ${stride2} +
            depth2 * ${stride3} + depth3 * ${stride4} + depth4;
            vec2 uv = uvFromFlat(${texNumR}, ${texNumC}, index);
            return sampleTexture(${name}, uv);
          }
        `;
    return new glsl_definitions_1.GlslLibRoutine(source, [
      'coordinates.uvFromFlat',
      'coordinates.sampleTexture',
      'coordinates.coordsToOffset',
    ]);
  }
  /**
   * This is the main function to map from the given texture coordinates (s,t)
   * to logical indices for the output
   * There will only be one single variation of this
   * Also see coordsToOffset and offsetToIndices for input-specific versions
   */
  toVec() {
    const output = this.context.outputTextureLayout;
    const rank = output.shape.length;
    const strides = output.strides;
    const xScale = output.width;
    const yScale = output.height;
    const stridesBlock = [];
    for (let i = 0; i < rank - 1; ++i) {
      stridesBlock.push(`
        c[${i}] = offset / ${strides[i]};`);
      stridesBlock.push(`
        offset -= c[${i}] * ${strides[i]};`);
    }
    stridesBlock.push(`
        c[${rank - 1}] = offset;`);
    const body = `
      void toVec(vec2 texCoords, out int c[${rank}]) {
        int offset = coordsToOffset(texCoords, ${xScale}, ${yScale});
        ${stridesBlock.join('')}
      }
      void toVec(int offset, out int c[${rank}]) {
        ${stridesBlock.join('')}
      }
    `;
    return { toVec: new glsl_definitions_1.GlslLibRoutine(body, ['coordinates.coordsToOffset']) };
  }
  /**
   * These are value getter functions generated for each input
   * Each function is hardwired to the name and dimensions of the input
   * An '_T' variation is also produced which accesses values as if the
   * input was transposed
   */
  valueFrom() {
    const result = {};
    this.context.programInfo.inputNames.forEach((name, i) => {
      const layout = this.context.inputTextureLayouts[i];
      const shape = layout.unpackedShape.length > 0 ? layout.unpackedShape : layout.shape;
      const rank = shape.length;
      let funcName = `_${name}`;
      result[funcName] = new glsl_definitions_1.GlslLibRoutine(
        this.getValueFromSingle(name, rank, layout.width, layout.height, false),
        [`shapeUtils.indicesToOffset${funcName}`, 'coordinates.offsetToCoords', 'fragcolor.getColorAsFloat'],
      );
      funcName = funcName + '_T';
      result[funcName] = new glsl_definitions_1.GlslLibRoutine(
        this.getValueFromSingle(name, rank, layout.width, layout.height, true),
        [`shapeUtils.indicesToOffset${funcName}`, 'coordinates.offsetToCoords', 'fragcolor.getColorAsFloat'],
      );
    });
    return result;
  }
  /**
   * Produces one value getter function for the name and rank given
   * If a transpose is set proper offsetToCoords mapping will be used
   * @param name name of the function
   * @param rank rank of the input
   * @param transpose whether or not should generate a transpose variation
   */
  getValueFromSingle(varName, rank, width, height, transpose) {
    let name = `_${varName}`;
    if (transpose) {
      name = name + '_T';
    }
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    return `
        float ${name}(int m[${rank}]) {
          int offset = indicesToOffset${name}(m);
          vec2 coords = offsetToCoords(offset, ${width}, ${height});
          float value = getColorAsFloat(${glsl.texture2D}(${varName}, coords));
          return value;
        }
        `;
  }
  /**
   * Produces a packed value getter function for the name and rank given
   * If a transpose is set proper offsetToCoords mapping will be used
   * @param name name of the function
   * @param rank rank of the input
   * @param transpose whether or not should generate a transpose variation
   */
  getPackedValueFrom(varName, rank, width, height, transpose) {
    let name = `_${varName}_Pack`;
    if (transpose) {
      name = name + '_T';
    }
    const glsl = (0, glsl_source_1.getGlsl)(this.context.glContext.version);
    return `
        vec4 ${name}(int m[${rank}]) {
          int offset = indicesToOffset_${varName}(m);
          vec2 coords = offsetToCoords(offset, ${width}, ${height});
          return ${glsl.texture2D}(${varName}, coords);
        }
        `;
  }
}
exports.CoordsGlslLib = CoordsGlslLib;
//# sourceMappingURL=glsl-coordinate-lib.js.map
