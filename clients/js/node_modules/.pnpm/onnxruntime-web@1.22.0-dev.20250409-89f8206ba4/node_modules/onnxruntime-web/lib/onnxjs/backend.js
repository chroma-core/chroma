'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.resolveBackend = exports.backend = void 0;
const backend_webgl_1 = require('./backends/backend-webgl');
// caches all initialized backend instances
const backendsCache = new Map();
exports.backend = {
  webgl: new backend_webgl_1.WebGLBackend(),
};
/**
 * Resolve a reference to the backend. If a hint is specified, the corresponding
 * backend will be used.
 */
async function resolveBackend(hint) {
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
exports.resolveBackend = resolveBackend;
async function tryLoadBackend(backendHint) {
  const backendObj = exports.backend;
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
function isBackend(obj) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const o = obj;
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
//# sourceMappingURL=backend.js.map
