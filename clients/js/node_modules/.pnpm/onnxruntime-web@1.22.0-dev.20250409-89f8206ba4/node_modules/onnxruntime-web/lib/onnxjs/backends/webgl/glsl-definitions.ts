// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { ProgramInfo, TextureLayout } from './types';
import { WebGLContext } from './webgl-context';

/* eslint-disable @typescript-eslint/naming-convention */
export enum FunctionType {
  ValueBased,
  Positional,
}
export interface GlslFunction<T extends FunctionType> {
  body: string;
  name: string;
  type: T;
}
export type GlslValueFunction = GlslFunction<FunctionType.ValueBased>;
export interface GlslPositionalFunction extends GlslFunction<FunctionType.Positional> {
  inputShape: readonly number[];
  outputShape: readonly number[];
}

export class GlslContext {
  constructor(
    public glContext: WebGLContext,
    public programInfo: ProgramInfo,
    public inputTextureLayouts: TextureLayout[],
    public outputTextureLayout: TextureLayout,
  ) {}
}
export abstract class GlslLib {
  constructor(public context: GlslContext) {}
  abstract getFunctions(): { [name: string]: GlslLibRoutine };
  abstract getCustomTypes(): { [name: string]: string };
}

// abstraction to represent a GLSL library routine and it's dependencies
export class GlslLibRoutine {
  constructor(
    public routineBody: string,
    public dependencies?: string[],
  ) {}
}

// abstraction to represent a GLSL library routine and it's dependencies AS GRAPH Nodes
// this level of abstraction is used to topologically sort routines before fragment shade inclusion
export class GlslLibRoutineNode {
  dependencies: GlslLibRoutineNode[];
  routineBody: string;
  constructor(
    public name: string,
    routineBody?: string,
    dependencies?: GlslLibRoutineNode[],
  ) {
    if (dependencies) {
      this.dependencies = dependencies;
    } else {
      this.dependencies = [];
    }

    if (routineBody) {
      this.routineBody = routineBody;
    }
  }
  addDependency(node: GlslLibRoutineNode) {
    if (node) {
      this.dependencies.push(node);
    }
  }
}

// topologically sort GLSL library routines (graph nodes abstraction) before shader script inclusion
export class TopologicalSortGlslRoutines {
  static returnOrderedNodes(nodes: GlslLibRoutineNode[]): GlslLibRoutineNode[] {
    if (!nodes || nodes.length === 0) {
      return [];
    }

    if (nodes.length === 1) {
      return nodes;
    }

    const cycleCheck = new Set<string>();
    const alreadyTraversed = new Set<string>();
    const result = new Array<GlslLibRoutineNode>();

    this.createOrderedNodes(nodes, cycleCheck, alreadyTraversed, result);
    return result;
  }

  private static createOrderedNodes(
    graphNodes: GlslLibRoutineNode[],
    cycleCheck: Set<string>,
    alreadyTraversed: Set<string>,
    result: GlslLibRoutineNode[],
  ) {
    for (let i = 0; i < graphNodes.length; ++i) {
      this.dfsTraverse(graphNodes[i], cycleCheck, alreadyTraversed, result);
    }
  }

  private static dfsTraverse(
    root: GlslLibRoutineNode,
    cycleCheck: Set<string>,
    alreadyTraversed: Set<string>,
    result: GlslLibRoutineNode[],
  ) {
    // if this root has already been traversed return
    if (!root || alreadyTraversed.has(root.name)) {
      return;
    }

    // cyclic dependency has been detected
    if (cycleCheck.has(root.name)) {
      throw new Error("Cyclic dependency detected. Can't topologically sort routines needed for shader.");
    }

    // hold this node to detect cycles if any
    cycleCheck.add(root.name);

    // traverse children in a dfs fashion
    const dependencies = root.dependencies;
    if (dependencies && dependencies.length > 0) {
      for (let i = 0; i < dependencies.length; ++i) {
        this.dfsTraverse(dependencies[i], cycleCheck, alreadyTraversed, result);
      }
    }

    // add to result holder
    result.push(root);

    // mark this node as traversed so that we don't traverse from this again
    alreadyTraversed.add(root.name);

    // release the hold
    cycleCheck.delete(root.name);
  }
}
