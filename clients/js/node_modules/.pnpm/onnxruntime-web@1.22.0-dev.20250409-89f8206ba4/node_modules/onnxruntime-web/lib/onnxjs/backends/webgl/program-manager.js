'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.ProgramManager = void 0;
const onnxruntime_common_1 = require('onnxruntime-common');
const instrument_1 = require('../../instrument');
const glsl_preprocessor_1 = require('./glsl-preprocessor');
const glsl_source_1 = require('./glsl-source');
/**
 * ProgramManager is the main class behind running computations
 * It builds ProgramInfo's into Artifacts
 * It compiles given ProgramInfo's into WebGL Prorams (cached as Artifacts)
 * Uses the artifact to run the computation by calling Draw on
 * the WebGL drawing buffer
 * ProgramManager automatically maps (binds) input variables to their
 * corresponding Location's in the binary program
 */
class ProgramManager {
  constructor(profiler, glContext, textureLayoutStrategy) {
    this.profiler = profiler;
    this.glContext = glContext;
    this.textureLayoutStrategy = textureLayoutStrategy;
    this.repo = new Map();
    this.attributesBound = false;
  }
  getArtifact(key) {
    return this.repo.get(key);
  }
  setArtifact(key, artifact) {
    this.repo.set(key, artifact);
  }
  run(buildArtifact, inputs, output) {
    this.profiler.event(
      'op',
      `ProgramManager.run ${buildArtifact.programInfo.name ?? 'unknown kernel'}`,
      () => {
        const gl = this.glContext.gl;
        const program = buildArtifact.program;
        gl.useProgram(program);
        try {
          this.bindOutput(output);
          if (!this.attributesBound) {
            this.bindAttributes(buildArtifact.attribLocations);
          }
          this.bindUniforms(buildArtifact.uniformLocations, buildArtifact.programInfo.variables ?? [], inputs);
        } catch (err) {
          instrument_1.Logger.error('ProgramManager', buildArtifact.programInfo.shaderSource);
          throw err;
        }
        this.profiler.event('backend', 'GlContext.draw()', () => {
          this.glContext.draw();
        });
      },
      this.glContext,
    );
  }
  dispose() {
    if (this.vertexShader) {
      this.glContext.deleteShader(this.vertexShader);
    }
    this.repo.forEach((a) => this.glContext.deleteProgram(a.program));
  }
  build(programInfo, inputTextureLayouts, outputTextureLayout) {
    return this.profiler.event('backend', 'ProgramManager.build', () => {
      const preprocessor = new glsl_preprocessor_1.GlslPreprocessor(
        this.glContext,
        programInfo,
        inputTextureLayouts,
        outputTextureLayout,
      );
      const fragScript = preprocessor.preprocess();
      const program = this.compile(fragScript);
      const artifact = {
        programInfo,
        program,
        uniformLocations: this.getUniformLocations(
          program,
          preprocessor.context.programInfo.inputNames,
          preprocessor.context.programInfo.variables,
        ),
        attribLocations: this.getAttribLocations(program),
      };
      return artifact;
    });
  }
  compile(fragShaderScript) {
    if (!this.vertexShader) {
      instrument_1.Logger.verbose('ProrgramManager', 'Compiling and caching Vertex shader for the first time');
      const vertexShaderScript = (0, glsl_source_1.getVertexShaderSource)(this.glContext.version);
      this.vertexShader = this.glContext.compileShader(vertexShaderScript, this.glContext.gl.VERTEX_SHADER);
    }
    if (onnxruntime_common_1.env.debug) {
      instrument_1.Logger.verbose(
        'ProrgramManager',
        `FragShader:
${fragShaderScript}
`,
      );
    }
    const fragShader = this.glContext.compileShader(fragShaderScript, this.glContext.gl.FRAGMENT_SHADER);
    const program = this.glContext.createProgram(this.vertexShader, fragShader);
    this.glContext.deleteShader(fragShader);
    return program;
  }
  bindOutput(td) {
    const width = td.width;
    const height = td.height;
    instrument_1.Logger.verbose(
      'ProrgramManager',
      `Binding output texture to Framebuffer: w/h=${width}/${height}, shape=${td.shape}, type=${td.tensor.type}`,
    );
    this.glContext.attachFramebuffer(td.texture, width, height);
  }
  bindAttributes(attribLocations) {
    const positionHandle = attribLocations.position;
    const textureCoordHandle = attribLocations.textureCoord;
    this.glContext.setVertexAttributes(positionHandle, textureCoordHandle);
    this.attributesBound = true;
  }
  bindUniforms(uniformLocations, variables, textures) {
    const gl = this.glContext.gl;
    let texturePosition = 0;
    for (const { name, type, location, arrayLength } of uniformLocations) {
      const value = variables.find((v) => v.name === name)?.data;
      if (type !== 'sampler2D' && !value) {
        throw new Error(`variable '${name}' does not have data defined in program info`);
      }
      switch (type) {
        case 'sampler2D':
          this.bindTexture(textures[texturePosition], location, texturePosition);
          texturePosition++;
          break;
        case 'float':
          if (arrayLength) {
            gl.uniform1fv(location, value);
          } else {
            gl.uniform1f(location, value);
          }
          break;
        case 'int':
          if (arrayLength) {
            gl.uniform1iv(location, value);
          } else {
            gl.uniform1i(location, value);
          }
          break;
        default:
          throw new Error(`Uniform not implemented: ${type}`);
      }
    }
  }
  bindTexture(td, uniformHandle, position) {
    this.glContext.bindTextureToUniform(td.texture, position, uniformHandle);
  }
  getAttribLocations(program) {
    return {
      position: this.getAttribLocation(program, 'position'),
      textureCoord: this.getAttribLocation(program, 'textureCoord'),
    };
  }
  getUniformLocations(program, samplers, variables) {
    const uniformLocations = [];
    if (samplers) {
      for (const sampler of samplers) {
        uniformLocations.push({
          name: sampler,
          type: 'sampler2D',
          location: this.getUniformLocation(program, sampler),
        });
      }
    }
    if (variables) {
      for (const variable of variables) {
        uniformLocations.push({ ...variable, location: this.getUniformLocation(program, variable.name) });
      }
    }
    return uniformLocations;
  }
  getUniformLocation(program, name) {
    const gl = this.glContext.gl;
    const reference = gl.getUniformLocation(program, name);
    if (reference === null) {
      throw new Error(`Uniform ${name} not found.`);
    }
    return reference;
  }
  getAttribLocation(program, name) {
    const gl = this.glContext.gl;
    const attributeLocation = gl.getAttribLocation(program, name);
    return attributeLocation;
  }
}
exports.ProgramManager = ProgramManager;
//# sourceMappingURL=program-manager.js.map
