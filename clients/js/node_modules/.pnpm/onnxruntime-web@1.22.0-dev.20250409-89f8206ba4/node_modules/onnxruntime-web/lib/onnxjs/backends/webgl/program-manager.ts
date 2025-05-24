// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from 'onnxruntime-common';

import { Logger, Profiler } from '../../instrument';

import { GlslPreprocessor } from './glsl-preprocessor';
import { getVertexShaderSource } from './glsl-source';
import { TextureLayoutStrategy } from './texture-layout-strategy';
import { Artifact, ProgramInfo, ProgramVariable, TextureData, TextureLayout, VariableInfo } from './types';
import { WebGLContext } from './webgl-context';

/**
 * ProgramManager is the main class behind running computations
 * It builds ProgramInfo's into Artifacts
 * It compiles given ProgramInfo's into WebGL Prorams (cached as Artifacts)
 * Uses the artifact to run the computation by calling Draw on
 * the WebGL drawing buffer
 * ProgramManager automatically maps (binds) input variables to their
 * corresponding Location's in the binary program
 */
export class ProgramManager {
  repo: Map<unknown, Artifact>; // this should be per-session object
  vertexShader: WebGLShader;
  attributesBound: boolean;

  constructor(
    public profiler: Readonly<Profiler>,
    public glContext: WebGLContext,
    public textureLayoutStrategy: TextureLayoutStrategy,
  ) {
    this.repo = new Map();
    this.attributesBound = false;
  }
  getArtifact(key: unknown): Artifact | undefined {
    return this.repo.get(key);
  }
  setArtifact(key: unknown, artifact: Artifact): void {
    this.repo.set(key, artifact);
  }
  run(buildArtifact: Artifact, inputs: TextureData[], output: TextureData): void {
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
          Logger.error('ProgramManager', buildArtifact.programInfo.shaderSource);
          throw err;
        }
        this.profiler.event('backend', 'GlContext.draw()', () => {
          this.glContext.draw();
        });
      },
      this.glContext,
    );
  }
  dispose(): void {
    if (this.vertexShader) {
      this.glContext.deleteShader(this.vertexShader);
    }
    this.repo.forEach((a) => this.glContext.deleteProgram(a.program));
  }
  build(programInfo: ProgramInfo, inputTextureLayouts: TextureLayout[], outputTextureLayout: TextureLayout): Artifact {
    return this.profiler.event('backend', 'ProgramManager.build', () => {
      const preprocessor = new GlslPreprocessor(this.glContext, programInfo, inputTextureLayouts, outputTextureLayout);
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
  protected compile(fragShaderScript: string): WebGLProgram {
    if (!this.vertexShader) {
      Logger.verbose('ProrgramManager', 'Compiling and caching Vertex shader for the first time');
      const vertexShaderScript = getVertexShaderSource(this.glContext.version);
      this.vertexShader = this.glContext.compileShader(vertexShaderScript, this.glContext.gl.VERTEX_SHADER);
    }
    if (env.debug) {
      Logger.verbose(
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
  bindOutput(td: TextureData): void {
    const width = td.width;
    const height = td.height;
    Logger.verbose(
      'ProrgramManager',
      `Binding output texture to Framebuffer: w/h=${width}/${height}, shape=${td.shape}, type=${td.tensor.type}`,
    );
    this.glContext.attachFramebuffer(td.texture, width, height);
  }
  bindAttributes(attribLocations: Artifact.AttribLocations): void {
    const positionHandle = attribLocations.position;
    const textureCoordHandle = attribLocations.textureCoord;
    this.glContext.setVertexAttributes(positionHandle, textureCoordHandle);
    this.attributesBound = true;
  }
  bindUniforms(
    uniformLocations: Artifact.UniformLocations,
    variables: ProgramVariable[],
    textures: TextureData[],
  ): void {
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
            gl.uniform1fv(location, value as number[]);
          } else {
            gl.uniform1f(location, value as number);
          }
          break;
        case 'int':
          if (arrayLength) {
            gl.uniform1iv(location, value as number[]);
          } else {
            gl.uniform1i(location, value as number);
          }
          break;
        default:
          throw new Error(`Uniform not implemented: ${type}`);
      }
    }
  }
  bindTexture(td: TextureData, uniformHandle: WebGLUniformLocation, position: number): void {
    this.glContext.bindTextureToUniform(td.texture, position, uniformHandle);
  }
  getAttribLocations(program: WebGLProgram): Artifact.AttribLocations {
    return {
      position: this.getAttribLocation(program, 'position'),
      textureCoord: this.getAttribLocation(program, 'textureCoord'),
    };
  }
  getUniformLocations(
    program: WebGLProgram,
    samplers?: string[],
    variables?: VariableInfo[],
  ): Artifact.UniformLocations {
    const uniformLocations: Artifact.UniformLocations = [];
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
  getUniformLocation(program: WebGLProgram, name: string): WebGLUniformLocation {
    const gl = this.glContext.gl;
    const reference = gl.getUniformLocation(program, name);
    if (reference === null) {
      throw new Error(`Uniform ${name} not found.`);
    }
    return reference;
  }
  getAttribLocation(program: WebGLProgram, name: string): number {
    const gl = this.glContext.gl;
    const attributeLocation: number = gl.getAttribLocation(program, name);
    return attributeLocation;
  }
}
