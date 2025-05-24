'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.WebGLBackend = void 0;
const onnxruntime_common_1 = require('onnxruntime-common');
const instrument_1 = require('../instrument');
const session_handler_1 = require('./webgl/session-handler');
const webgl_context_factory_1 = require('./webgl/webgl-context-factory');
/**
 * WebGLBackend is the entry point for all WebGL opeartions
 * When it starts it created the WebGLRenderingContext
 * and other main framework components such as Program and Texture Managers
 */
class WebGLBackend {
  get contextId() {
    return onnxruntime_common_1.env.webgl.contextId;
  }
  set contextId(value) {
    onnxruntime_common_1.env.webgl.contextId = value;
  }
  get matmulMaxBatchSize() {
    return onnxruntime_common_1.env.webgl.matmulMaxBatchSize;
  }
  set matmulMaxBatchSize(value) {
    onnxruntime_common_1.env.webgl.matmulMaxBatchSize = value;
  }
  get textureCacheMode() {
    return onnxruntime_common_1.env.webgl.textureCacheMode;
  }
  set textureCacheMode(value) {
    onnxruntime_common_1.env.webgl.textureCacheMode = value;
  }
  get pack() {
    return onnxruntime_common_1.env.webgl.pack;
  }
  set pack(value) {
    onnxruntime_common_1.env.webgl.pack = value;
  }
  get async() {
    return onnxruntime_common_1.env.webgl.async;
  }
  set async(value) {
    onnxruntime_common_1.env.webgl.async = value;
  }
  initialize() {
    try {
      this.glContext = (0, webgl_context_factory_1.createWebGLContext)(this.contextId);
      if (typeof this.matmulMaxBatchSize !== 'number') {
        this.matmulMaxBatchSize = 16;
      }
      if (typeof this.textureCacheMode !== 'string') {
        this.textureCacheMode = 'full';
      }
      if (typeof this.pack !== 'boolean') {
        this.pack = false;
      }
      if (typeof this.async !== 'boolean') {
        this.async = false;
      }
      instrument_1.Logger.setWithEnv(onnxruntime_common_1.env);
      if (!onnxruntime_common_1.env.webgl.context) {
        Object.defineProperty(onnxruntime_common_1.env.webgl, 'context', { value: this.glContext.gl });
      }
      instrument_1.Logger.verbose(
        'WebGLBackend',
        `Created WebGLContext: ${typeof this.glContext} with matmulMaxBatchSize: ${this.matmulMaxBatchSize}; textureCacheMode: ${this.textureCacheMode}; pack: ${this.pack}; async: ${this.async}.`,
      );
      return true;
    } catch (e) {
      instrument_1.Logger.warning('WebGLBackend', `Unable to initialize WebGLBackend. ${e}`);
      return false;
    }
  }
  createSessionHandler(context) {
    return new session_handler_1.WebGLSessionHandler(this, context);
  }
  dispose() {
    this.glContext.dispose();
  }
}
exports.WebGLBackend = WebGLBackend;
//# sourceMappingURL=backend-webgl.js.map
