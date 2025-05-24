// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

export * from 'onnxruntime-common';
export { listSupportedBackends } from './backend';
import { registerBackend, env } from 'onnxruntime-common';
import { version } from './version';
import { onnxruntimeBackend, listSupportedBackends } from './backend';

const backends = listSupportedBackends();
for (const backend of backends) {
  registerBackend(backend.name, onnxruntimeBackend, 100);
}

Object.defineProperty(env.versions, 'node', { value: version, enumerable: true });
