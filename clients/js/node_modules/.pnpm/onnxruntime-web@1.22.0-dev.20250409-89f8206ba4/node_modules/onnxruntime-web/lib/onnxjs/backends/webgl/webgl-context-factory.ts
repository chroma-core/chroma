// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Logger } from '../../instrument';

import { WebGLContext } from './webgl-context';

const cache: { [contextId: string]: WebGLContext } = {};

/**
 * This factory function creates proper WebGLRenderingContext based on
 * the current browsers capabilities
 * The order is from higher/most recent versions to most basic
 */
export function createWebGLContext(contextId?: 'webgl' | 'webgl2'): WebGLContext {
  let context: WebGLContext | undefined;
  if ((!contextId || contextId === 'webgl2') && 'webgl2' in cache) {
    context = cache.webgl2;
  } else if ((!contextId || contextId === 'webgl') && 'webgl' in cache) {
    context = cache.webgl;
  }

  if (!context) {
    try {
      // try to create webgl context from an offscreen canvas
      const offscreenCanvas = createOffscreenCanvas();
      context = createNewWebGLContext(offscreenCanvas, contextId);
    } catch (e) {
      // if failed, fallback to try to use a normal canvas element
      const canvas = createCanvas();
      context = createNewWebGLContext(canvas, contextId);
    }
  }

  contextId = contextId || context.version === 1 ? 'webgl' : 'webgl2';
  const gl = context.gl;

  cache[contextId] = context;

  if (gl.isContextLost()) {
    delete cache[contextId];
    return createWebGLContext(contextId);
  }

  gl.disable(gl.DEPTH_TEST);
  gl.disable(gl.STENCIL_TEST);
  gl.disable(gl.BLEND);
  gl.disable(gl.DITHER);
  gl.disable(gl.POLYGON_OFFSET_FILL);
  gl.disable(gl.SAMPLE_COVERAGE);
  gl.enable(gl.SCISSOR_TEST);
  gl.enable(gl.CULL_FACE);
  gl.cullFace(gl.BACK);

  return context;
}

export function createNewWebGLContext(canvas: HTMLCanvasElement, contextId?: 'webgl' | 'webgl2'): WebGLContext {
  const contextAttributes: WebGLContextAttributes = {
    alpha: false,
    depth: false,
    antialias: false,
    stencil: false,
    preserveDrawingBuffer: false,
    premultipliedAlpha: false,
    failIfMajorPerformanceCaveat: false,
  };
  let gl: WebGLRenderingContext | null;
  const ca = contextAttributes;
  if (!contextId || contextId === 'webgl2') {
    gl = canvas.getContext('webgl2', ca);
    if (gl) {
      try {
        return new WebGLContext(gl, 2);
      } catch (err) {
        Logger.warning('GlContextFactory', `failed to create WebGLContext using contextId 'webgl2'. Error: ${err}`);
      }
    }
  }
  if (!contextId || contextId === 'webgl') {
    gl = canvas.getContext('webgl', ca) || (canvas.getContext('experimental-webgl', ca) as WebGLRenderingContext);
    if (gl) {
      try {
        return new WebGLContext(gl, 1);
      } catch (err) {
        Logger.warning(
          'GlContextFactory',
          `failed to create WebGLContext using contextId 'webgl' or 'experimental-webgl'. Error: ${err}`,
        );
      }
    }
  }

  throw new Error('WebGL is not supported');
}

// eslint-disable-next-line @typescript-eslint/naming-convention
declare let OffscreenCanvas: { new (width: number, height: number): HTMLCanvasElement };

function createCanvas(): HTMLCanvasElement {
  if (typeof document === 'undefined') {
    throw new TypeError('failed to create canvas: document is not supported');
  }
  const canvas: HTMLCanvasElement = document.createElement('canvas');
  canvas.width = 1;
  canvas.height = 1;
  return canvas;
}

function createOffscreenCanvas(): HTMLCanvasElement {
  if (typeof OffscreenCanvas === 'undefined') {
    throw new TypeError('failed to create offscreen canvas: OffscreenCanvas is not supported');
  }
  return new OffscreenCanvas(1, 1);
}
