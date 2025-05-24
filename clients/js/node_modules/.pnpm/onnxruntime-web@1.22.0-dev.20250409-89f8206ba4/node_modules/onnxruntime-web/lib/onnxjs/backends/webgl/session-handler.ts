// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { SessionHandler } from '../../backend';
import { Graph } from '../../graph';
import { Logger } from '../../instrument';
import { Operator } from '../../operators';
import { OpSet, resolveOperator } from '../../opset';
import { Session } from '../../session';
import { Tensor } from '../../tensor';
import { WebGLBackend } from '../backend-webgl';

import { WebGLInferenceHandler } from './inference-handler';
import { WEBGL_OP_RESOLVE_RULES } from './op-resolve-rules';
import { ProgramManager } from './program-manager';
import { PreferLogicalStrategy, TextureLayoutStrategy } from './texture-layout-strategy';
import { TextureManager } from './texture-manager';
import { TextureData } from './types';

export class WebGLSessionHandler implements SessionHandler {
  programManager: ProgramManager;
  textureManager: TextureManager;
  layoutStrategy: TextureLayoutStrategy;
  packedTextureDataCache: Map<Tensor.Id, TextureData>;
  unpackedTextureDataCache: Map<Tensor.Id, TextureData>;
  pack2unpackMap: Map<Tensor.Id, Tensor.Id>;
  unpack2packMap: Map<Tensor.Id, Tensor.Id>;
  initializers: Set<Tensor.Id>;
  pack?: boolean;

  constructor(
    public readonly backend: WebGLBackend,
    public readonly context: Session.Context,
  ) {
    this.layoutStrategy = new PreferLogicalStrategy(backend.glContext.maxTextureSize);
    this.programManager = new ProgramManager(this.context.profiler, backend.glContext, this.layoutStrategy);
    this.textureManager = new TextureManager(backend.glContext, this.layoutStrategy, this.context.profiler, {
      reuseTextures: backend.textureCacheMode === 'full',
    });
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache = new Map();
    this.pack = backend.pack;
    this.pack2unpackMap = new Map();
    this.unpack2packMap = new Map();
  }

  createInferenceHandler() {
    return new WebGLInferenceHandler(this);
  }
  onGraphInitialized(graph: Graph): void {
    const initializers = graph
      .getValues()
      .filter((v) => v.from === -1 && v.tensor)
      .map((v) => v.tensor!.dataId);
    this.initializers = new Set(initializers);
  }
  isInitializer(tensorId: Tensor.Id): boolean {
    return this.initializers ? this.initializers.has(tensorId) : false;
  }
  addInitializer(tensorId: Tensor.Id): void {
    this.initializers.add(tensorId);
  }
  getTextureData(tensorId: Tensor.Id, isPacked: boolean): TextureData | undefined {
    if (isPacked) {
      return this.packedTextureDataCache.get(tensorId);
    } else {
      return this.unpackedTextureDataCache.get(tensorId);
    }
  }
  setTextureData(tensorId: Tensor.Id, textureData: TextureData, isPacked = false): void {
    Logger.verbose('WebGLSessionHandler', 'Storing Texture data in cache');
    if (isPacked) {
      this.packedTextureDataCache.set(tensorId, textureData);
    } else {
      this.unpackedTextureDataCache.set(tensorId, textureData);
    }
  }
  dispose(): void {
    this.programManager.dispose();
    this.textureManager.clearActiveTextures();
    this.packedTextureDataCache.forEach((td) => this.textureManager.releaseTexture(td, true));
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache.forEach((td) => this.textureManager.releaseTexture(td, true));
    this.unpackedTextureDataCache = new Map();
  }
  resolve(node: Graph.Node, opsets: readonly OpSet[], graph: Graph): Operator {
    const op = resolveOperator(node, opsets, WEBGL_OP_RESOLVE_RULES);
    return { impl: op.opImpl, context: op.opInit ? op.opInit(node, graph) : node };
  }
}
