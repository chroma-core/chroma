// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { GlslContext, GlslLib, GlslLibRoutineNode, TopologicalSortGlslRoutines } from './glsl-definitions';
import { replaceInlines } from './glsl-function-inliner';
import { glslRegistry } from './glsl-registered-libs';
import { getDefaultFragShaderMain, getFragShaderPreamble } from './glsl-source';
import { ProgramInfo, TextureLayout, VariableInfo } from './types';
import { WebGLContext } from './webgl-context';

/**
 * Preprocessor for the additions to the GLSL language
 * It deals with:
 *  @include directives
 *  @inline
 *  Loop unrolling (not implemented)
 *  Macro resolution (not implemented)
 */
export class GlslPreprocessor {
  readonly context: GlslContext;
  readonly libs: { [name: string]: GlslLib } = {};
  readonly glslLibRoutineDependencyGraph: { [routineName: string]: GlslLibRoutineNode } = {};

  constructor(
    glContext: WebGLContext,
    programInfo: ProgramInfo,
    inputTextureLayouts: TextureLayout[],
    outputTextureLayout: TextureLayout,
  ) {
    this.context = new GlslContext(glContext, programInfo, inputTextureLayouts, outputTextureLayout);

    // construct GlslLibs
    Object.keys(glslRegistry).forEach((name: string) => {
      const lib = new glslRegistry[name](this.context);
      this.libs[name] = lib;
    });

    // construct GlslRoutineDependencyGraph
    const map = this.glslLibRoutineDependencyGraph;
    for (const libName in this.libs) {
      const lib = this.libs[libName];
      const routinesInLib = lib.getFunctions();
      for (const routine in routinesInLib) {
        const key = libName + '.' + routine;
        let currentNode: GlslLibRoutineNode;
        if (map[key]) {
          currentNode = map[key];
          currentNode.routineBody = routinesInLib[routine].routineBody;
        } else {
          currentNode = new GlslLibRoutineNode(key, routinesInLib[routine].routineBody);
          map[key] = currentNode;
        }
        const dependencies = routinesInLib[routine].dependencies;
        if (dependencies) {
          for (let i = 0; i < dependencies.length; ++i) {
            if (!map[dependencies[i]]) {
              const node = new GlslLibRoutineNode(dependencies[i]);
              map[dependencies[i]] = node;
              currentNode.addDependency(node);
            } else {
              currentNode.addDependency(map[dependencies[i]]);
            }
          }
        }
      }
    }
  }

  preprocess(): string {
    const programInfo = this.context.programInfo;
    let source = programInfo.shaderSource;

    // append main() function
    if (!this.context.programInfo.hasMain) {
      source = `${source}
      ${getDefaultFragShaderMain(this.context.glContext.version, this.context.outputTextureLayout.shape.length)}`;
    }
    // replace inlines
    source = replaceInlines(source);

    // concat final source string
    return `${getFragShaderPreamble(this.context.glContext.version)}
    ${this.getUniforms(programInfo.inputNames, programInfo.variables)}
    ${this.getImports(source)}
    ${source}`;
  }

  protected getImports(script: string): string {
    const routinesIncluded = this.selectGlslLibRoutinesToBeIncluded(script);

    if (routinesIncluded.length === 0) {
      return '';
    }

    let routines = '';
    for (let i = 0; i < routinesIncluded.length; ++i) {
      if (routinesIncluded[i].routineBody) {
        routines += routinesIncluded[i].routineBody + '\n';
      } else {
        throw new Error(`Missing body for the Glsl Library routine: ${routinesIncluded[i].name}`);
      }
    }

    return routines;
  }
  private selectGlslLibRoutinesToBeIncluded(script: string): GlslLibRoutineNode[] {
    const nodes: GlslLibRoutineNode[] = [];

    Object.keys(this.glslLibRoutineDependencyGraph).forEach((classAndRoutine) => {
      const routine = classAndRoutine.split('.')[1];
      if (script.indexOf(routine) !== -1) {
        nodes.push(this.glslLibRoutineDependencyGraph[classAndRoutine]);
      }
    });

    return TopologicalSortGlslRoutines.returnOrderedNodes(nodes);
  }

  protected getUniforms(samplers?: string[], variables?: VariableInfo[]): string {
    const uniformLines: string[] = [];
    if (samplers) {
      for (const sampler of samplers) {
        uniformLines.push(`uniform sampler2D ${sampler};`);
      }
    }
    if (variables) {
      for (const variable of variables) {
        uniformLines.push(
          `uniform ${variable.type} ${variable.name}${variable.arrayLength ? `[${variable.arrayLength}]` : ''};`,
        );
      }
    }
    return uniformLines.join('\n');
  }
}
