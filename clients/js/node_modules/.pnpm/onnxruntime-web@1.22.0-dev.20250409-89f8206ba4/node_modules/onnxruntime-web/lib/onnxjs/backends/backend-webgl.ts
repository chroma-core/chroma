// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from 'onnxruntime-common';

import { Backend, SessionHandler } from '../backend';
import { Logger } from '../instrument';
import { Session } from '../session';

import { WebGLSessionHandler } from './webgl/session-handler';
import { WebGLContext } from './webgl/webgl-context';
import { createWebGLContext } from './webgl/webgl-context-factory';

/**
 * WebGLBackend is the entry point for all WebGL opeartions
 * When it starts it created the WebGLRenderingContext
 * and other main framework components such as Program and Texture Managers
 */
export class WebGLBackend implements Backend {
  glContext: WebGLContext;

  get contextId(): 'webgl' | 'webgl2' | undefined {
    return env.webgl.contextId;
  }
  set contextId(value: 'webgl' | 'webgl2' | undefined) {
    env.webgl.contextId = value;
  }

  get matmulMaxBatchSize(): number | undefined {
    return env.webgl.matmulMaxBatchSize;
  }
  set matmulMaxBatchSize(value: number | undefined) {
    env.webgl.matmulMaxBatchSize = value;
  }

  get textureCacheMode(): 'initializerOnly' | 'full' | undefined {
    return env.webgl.textureCacheMode;
  }
  set textureCacheMode(value: 'initializerOnly' | 'full' | undefined) {
    env.webgl.textureCacheMode = value;
  }

  get pack(): boolean | undefined {
    return env.webgl.pack;
  }
  set pack(value: boolean | undefined) {
    env.webgl.pack = value;
  }

  get async(): boolean | undefined {
    return env.webgl.async;
  }
  set async(value: boolean | undefined) {
    env.webgl.async = value;
  }

  initialize(): boolean {
    try {
      this.glContext = createWebGLContext(this.contextId);
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

      Logger.setWithEnv(env);

      if (!env.webgl.context) {
        Object.defineProperty(env.webgl, 'context', { value: this.glContext.gl });
      }

      Logger.verbose(
        'WebGLBackend',
        `Created WebGLContext: ${typeof this.glContext} with matmulMaxBatchSize: ${
          this.matmulMaxBatchSize
        }; textureCacheMode: ${this.textureCacheMode}; pack: ${this.pack}; async: ${this.async}.`,
      );
      return true;
    } catch (e) {
      Logger.warning('WebGLBackend', `Unable to initialize WebGLBackend. ${e}`);
      return false;
    }
  }
  createSessionHandler(context: Session.Context): SessionHandler {
    return new WebGLSessionHandler(this, context);
  }
  dispose(): void {
    this.glContext.dispose();
  }
}
