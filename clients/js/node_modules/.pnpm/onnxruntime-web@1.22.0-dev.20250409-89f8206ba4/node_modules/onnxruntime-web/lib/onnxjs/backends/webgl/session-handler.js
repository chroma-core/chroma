'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.WebGLSessionHandler = void 0;
const instrument_1 = require('../../instrument');
const opset_1 = require('../../opset');
const inference_handler_1 = require('./inference-handler');
const op_resolve_rules_1 = require('./op-resolve-rules');
const program_manager_1 = require('./program-manager');
const texture_layout_strategy_1 = require('./texture-layout-strategy');
const texture_manager_1 = require('./texture-manager');
class WebGLSessionHandler {
  constructor(backend, context) {
    this.backend = backend;
    this.context = context;
    this.layoutStrategy = new texture_layout_strategy_1.PreferLogicalStrategy(backend.glContext.maxTextureSize);
    this.programManager = new program_manager_1.ProgramManager(
      this.context.profiler,
      backend.glContext,
      this.layoutStrategy,
    );
    this.textureManager = new texture_manager_1.TextureManager(
      backend.glContext,
      this.layoutStrategy,
      this.context.profiler,
      {
        reuseTextures: backend.textureCacheMode === 'full',
      },
    );
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache = new Map();
    this.pack = backend.pack;
    this.pack2unpackMap = new Map();
    this.unpack2packMap = new Map();
  }
  createInferenceHandler() {
    return new inference_handler_1.WebGLInferenceHandler(this);
  }
  onGraphInitialized(graph) {
    const initializers = graph
      .getValues()
      .filter((v) => v.from === -1 && v.tensor)
      .map((v) => v.tensor.dataId);
    this.initializers = new Set(initializers);
  }
  isInitializer(tensorId) {
    return this.initializers ? this.initializers.has(tensorId) : false;
  }
  addInitializer(tensorId) {
    this.initializers.add(tensorId);
  }
  getTextureData(tensorId, isPacked) {
    if (isPacked) {
      return this.packedTextureDataCache.get(tensorId);
    } else {
      return this.unpackedTextureDataCache.get(tensorId);
    }
  }
  setTextureData(tensorId, textureData, isPacked = false) {
    instrument_1.Logger.verbose('WebGLSessionHandler', 'Storing Texture data in cache');
    if (isPacked) {
      this.packedTextureDataCache.set(tensorId, textureData);
    } else {
      this.unpackedTextureDataCache.set(tensorId, textureData);
    }
  }
  dispose() {
    this.programManager.dispose();
    this.textureManager.clearActiveTextures();
    this.packedTextureDataCache.forEach((td) => this.textureManager.releaseTexture(td, true));
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache.forEach((td) => this.textureManager.releaseTexture(td, true));
    this.unpackedTextureDataCache = new Map();
  }
  resolve(node, opsets, graph) {
    const op = (0, opset_1.resolveOperator)(node, opsets, op_resolve_rules_1.WEBGL_OP_RESOLVE_RULES);
    return { impl: op.opImpl, context: op.opInit ? op.opInit(node, graph) : node };
  }
}
exports.WebGLSessionHandler = WebGLSessionHandler;
//# sourceMappingURL=session-handler.js.map
