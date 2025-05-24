// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Backend } from './backend.js';
import { InferenceSession } from './inference-session.js';

interface BackendInfo {
  backend: Backend;
  priority: number;

  initPromise?: Promise<void>;
  initialized?: boolean;
  aborted?: boolean;
  error?: string;
}

const backends: Map<string, BackendInfo> = new Map();
const backendsSortedByPriority: string[] = [];

/**
 * Register a backend.
 *
 * @param name - the name as a key to lookup as an execution provider.
 * @param backend - the backend object.
 * @param priority - an integer indicating the priority of the backend. Higher number means higher priority. if priority
 * < 0, it will be considered as a 'beta' version and will not be used as a fallback backend by default.
 *
 * @ignore
 */
export const registerBackend = (name: string, backend: Backend, priority: number): void => {
  if (backend && typeof backend.init === 'function' && typeof backend.createInferenceSessionHandler === 'function') {
    const currentBackend = backends.get(name);
    if (currentBackend === undefined) {
      backends.set(name, { backend, priority });
    } else if (currentBackend.priority > priority) {
      // same name is already registered with a higher priority. skip registeration.
      return;
    } else if (currentBackend.priority === priority) {
      if (currentBackend.backend !== backend) {
        throw new Error(`cannot register backend "${name}" using priority ${priority}`);
      }
    }

    if (priority >= 0) {
      const i = backendsSortedByPriority.indexOf(name);
      if (i !== -1) {
        backendsSortedByPriority.splice(i, 1);
      }

      for (let i = 0; i < backendsSortedByPriority.length; i++) {
        if (backends.get(backendsSortedByPriority[i])!.priority <= priority) {
          backendsSortedByPriority.splice(i, 0, name);
          return;
        }
      }
      backendsSortedByPriority.push(name);
    }
    return;
  }

  throw new TypeError('not a valid backend');
};

/**
 * Try to resolve and initialize a backend.
 *
 * @param backendName - the name of the backend.
 * @returns the backend instance if resolved and initialized successfully, or an error message if failed.
 */
const tryResolveAndInitializeBackend = async (backendName: string): Promise<Backend | string> => {
  const backendInfo = backends.get(backendName);
  if (!backendInfo) {
    return 'backend not found.';
  }

  if (backendInfo.initialized) {
    return backendInfo.backend;
  } else if (backendInfo.aborted) {
    return backendInfo.error!;
  } else {
    const isInitializing = !!backendInfo.initPromise;
    try {
      if (!isInitializing) {
        backendInfo.initPromise = backendInfo.backend.init(backendName);
      }
      await backendInfo.initPromise;
      backendInfo.initialized = true;
      return backendInfo.backend;
    } catch (e) {
      if (!isInitializing) {
        backendInfo.error = `${e}`;
        backendInfo.aborted = true;
      }
      return backendInfo.error!;
    } finally {
      delete backendInfo.initPromise;
    }
  }
};

/**
 * Resolve execution providers from the specific session options.
 *
 * @param options - the session options object.
 * @returns a promise that resolves to a tuple of an initialized backend instance and a session options object with
 * filtered EP list.
 *
 * @ignore
 */
export const resolveBackendAndExecutionProviders = async (
  options: InferenceSession.SessionOptions,
): Promise<[backend: Backend, options: InferenceSession.SessionOptions]> => {
  // extract backend hints from session options
  const eps = options.executionProviders || [];
  const backendHints = eps.map((i) => (typeof i === 'string' ? i : i.name));
  const backendNames = backendHints.length === 0 ? backendsSortedByPriority : backendHints;

  // try to resolve and initialize all requested backends
  let backend: Backend | undefined;
  const errors = [];
  const availableBackendNames = new Set<string>();
  for (const backendName of backendNames) {
    const resolveResult = await tryResolveAndInitializeBackend(backendName);
    if (typeof resolveResult === 'string') {
      errors.push({ name: backendName, err: resolveResult });
    } else {
      if (!backend) {
        backend = resolveResult;
      }
      if (backend === resolveResult) {
        availableBackendNames.add(backendName);
      }
    }
  }

  // if no backend is available, throw error.
  if (!backend) {
    throw new Error(`no available backend found. ERR: ${errors.map((e) => `[${e.name}] ${e.err}`).join(', ')}`);
  }

  // for each explicitly requested backend, if it's not available, output warning message.
  for (const { name, err } of errors) {
    if (backendHints.includes(name)) {
      // eslint-disable-next-line no-console
      console.warn(
        `removing requested execution provider "${name}" from session options because it is not available: ${err}`,
      );
    }
  }

  const filteredEps = eps.filter((i) => availableBackendNames.has(typeof i === 'string' ? i : i.name));

  return [
    backend,
    new Proxy(options, {
      get: (target, prop) => {
        if (prop === 'executionProviders') {
          return filteredEps;
        }
        return Reflect.get(target, prop);
      },
    }),
  ];
};
