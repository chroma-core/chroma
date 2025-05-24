'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.GlslPreprocessor = void 0;
const glsl_definitions_1 = require('./glsl-definitions');
const glsl_function_inliner_1 = require('./glsl-function-inliner');
const glsl_registered_libs_1 = require('./glsl-registered-libs');
const glsl_source_1 = require('./glsl-source');
/**
 * Preprocessor for the additions to the GLSL language
 * It deals with:
 *  @include directives
 *  @inline
 *  Loop unrolling (not implemented)
 *  Macro resolution (not implemented)
 */
class GlslPreprocessor {
  constructor(glContext, programInfo, inputTextureLayouts, outputTextureLayout) {
    this.libs = {};
    this.glslLibRoutineDependencyGraph = {};
    this.context = new glsl_definitions_1.GlslContext(glContext, programInfo, inputTextureLayouts, outputTextureLayout);
    // construct GlslLibs
    Object.keys(glsl_registered_libs_1.glslRegistry).forEach((name) => {
      const lib = new glsl_registered_libs_1.glslRegistry[name](this.context);
      this.libs[name] = lib;
    });
    // construct GlslRoutineDependencyGraph
    const map = this.glslLibRoutineDependencyGraph;
    for (const libName in this.libs) {
      const lib = this.libs[libName];
      const routinesInLib = lib.getFunctions();
      for (const routine in routinesInLib) {
        const key = libName + '.' + routine;
        let currentNode;
        if (map[key]) {
          currentNode = map[key];
          currentNode.routineBody = routinesInLib[routine].routineBody;
        } else {
          currentNode = new glsl_definitions_1.GlslLibRoutineNode(key, routinesInLib[routine].routineBody);
          map[key] = currentNode;
        }
        const dependencies = routinesInLib[routine].dependencies;
        if (dependencies) {
          for (let i = 0; i < dependencies.length; ++i) {
            if (!map[dependencies[i]]) {
              const node = new glsl_definitions_1.GlslLibRoutineNode(dependencies[i]);
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
  preprocess() {
    const programInfo = this.context.programInfo;
    let source = programInfo.shaderSource;
    // append main() function
    if (!this.context.programInfo.hasMain) {
      source = `${source}
      ${(0, glsl_source_1.getDefaultFragShaderMain)(this.context.glContext.version, this.context.outputTextureLayout.shape.length)}`;
    }
    // replace inlines
    source = (0, glsl_function_inliner_1.replaceInlines)(source);
    // concat final source string
    return `${(0, glsl_source_1.getFragShaderPreamble)(this.context.glContext.version)}
    ${this.getUniforms(programInfo.inputNames, programInfo.variables)}
    ${this.getImports(source)}
    ${source}`;
  }
  getImports(script) {
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
  selectGlslLibRoutinesToBeIncluded(script) {
    const nodes = [];
    Object.keys(this.glslLibRoutineDependencyGraph).forEach((classAndRoutine) => {
      const routine = classAndRoutine.split('.')[1];
      if (script.indexOf(routine) !== -1) {
        nodes.push(this.glslLibRoutineDependencyGraph[classAndRoutine]);
      }
    });
    return glsl_definitions_1.TopologicalSortGlslRoutines.returnOrderedNodes(nodes);
  }
  getUniforms(samplers, variables) {
    const uniformLines = [];
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
exports.GlslPreprocessor = GlslPreprocessor;
//# sourceMappingURL=glsl-preprocessor.js.map
