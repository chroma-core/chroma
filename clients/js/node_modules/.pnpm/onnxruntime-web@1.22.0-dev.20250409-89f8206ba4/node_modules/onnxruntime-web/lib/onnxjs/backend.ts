// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { WebGLBackend } from './backends/backend-webgl';
import { Graph } from './graph';
import { Operator } from './operators';
import { OpSet } from './opset';
import { Session } from './session';

export interface InferenceHandler {
  /**
   * dispose the inference handler. it will be called as the last step in Session.run()
   */
  dispose(): void;
}

export interface SessionHandler {
  /**
   * transform the graph at initialization time
   * @param graphTransformer the graph transformer to manipulate the model graph
   */
  transformGraph?(graphTransformer: Graph.Transformer): void;

  /**
   * create an instance of InferenceHandler to use in a Session.run() call
   */
  createInferenceHandler(): InferenceHandler;

  /**
   * dispose the session handler. it will be called when a session is being disposed explicitly
   */
  dispose(): void;

  /**
   * Resolves the operator from the name and opset version; backend specific
   * @param node the node to resolve
   * @param opsets a list of opsets that exported from the model
   * @param graph the completely initialized graph
   */
  resolve(node: Graph.Node, opsets: readonly OpSet[], graph: Graph): Operator;

  /**
   * This method let's the sessionHandler know that the graph initialization is complete
   * @param graph the completely initialized graph
   */
  onGraphInitialized?(graph: Graph): void;

  /**
   * a reference to the corresponding backend
   */
  readonly backend: Backend;

  /**
   * a reference to the session context
   */
  readonly context: Session.Context;
}

export interface Backend {
  /**
   * initialize the backend. will be called only once, when the first time the
   * backend it to be used
   */
  initialize(): boolean | Promise<boolean>;

  /**
   * create an instance of SessionHandler to use in a Session object's lifecycle
   */
  createSessionHandler(context: Session.Context): SessionHandler;

  /**
   * dispose the backend. currently this will not be called
   */
  dispose(): void;
}

// caches all initialized backend instances
const backendsCache: Map<string, Backend> = new Map();

export const backend: { [name: string]: Backend } = {
  webgl: new WebGLBackend(),
};

/**
 * Resolve a reference to the backend. If a hint is specified, the corresponding
 * backend will be used.
 */
export async function resolveBackend(hint?: string | readonly string[]): Promise<Backend> {
  if (!hint) {
    return resolveBackend(['webgl']);
  } else {
    const hints = typeof hint === 'string' ? [hint] : hint;

    for (const backendHint of hints) {
      const cache = backendsCache.get(backendHint);
      if (cache) {
        return cache;
      }

      const backend = await tryLoadBackend(backendHint);
      if (backend) {
        return backend;
      }
    }
  }

  throw new Error('no available backend to use');
}

async function tryLoadBackend(backendHint: string): Promise<Backend | undefined> {
  const backendObj = backend;

  if (typeof backendObj[backendHint] !== 'undefined' && isBackend(backendObj[backendHint])) {
    const backend = backendObj[backendHint];
    let init = backend.initialize();
    if (typeof init === 'object' && 'then' in init) {
      init = await init;
    }
    if (init) {
      backendsCache.set(backendHint, backend);
      return backend;
    }
  }

  return undefined;
}

function isBackend(obj: unknown) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o = obj as any;

  // check if an object is a Backend instance
  if (
    'initialize' in o &&
    typeof o.initialize === 'function' && // initialize()
    'createSessionHandler' in o &&
    typeof o.createSessionHandler === 'function' && // createSessionHandler()
    'dispose' in o &&
    typeof o.dispose === 'function' // dispose()
  ) {
    return true;
  }

  return false;
}

export type BackendType = Backend;
export type SessionHandlerType = ReturnType<BackendType['createSessionHandler']>;
export type InferenceHandlerType = ReturnType<SessionHandlerType['createInferenceHandler']>;
