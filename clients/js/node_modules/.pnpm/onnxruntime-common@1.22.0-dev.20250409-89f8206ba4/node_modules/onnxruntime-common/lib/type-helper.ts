// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * A helper type to get certain types if they are declared in global scope.
 *
 * For example, if you installed "@webgpu/types" as a dev dependency, then `TryGetTypeIfDeclared<'GPUDevice'>` will
 * be type `GPUDevice`, otherwise it will be type `unknown`.
 *
 *
 * We don't want to introduce "@webgpu/types" as a dependency of this package because:
 *
 * (1) For JavaScript users, it's not needed. For TypeScript users, they can install it as dev dependency themselves.
 *
 * (2) because "@webgpu/types" requires "@types/dom-webcodecs" as peer dependency when using TypeScript < v5.1 and its
 * version need to be chosen carefully according to the TypeScript version being used. This means so far there is not a
 * way to keep every TypeScript version happy. It turns out that we will easily broke users on some TypeScript version.
 *
 * for more info see https://github.com/gpuweb/types/issues/127
 *
 * Update (2024-08-07): The reason (2) may be no longer valid. Most people should be using TypeScript >= 5.1 by now.
 * However, we are still not sure whether introducing "@webgpu/types" as direct dependency is a good idea. We find this
 * type helper is useful for TypeScript users.
 *
 * @ignore
 */
export type TryGetGlobalType<Name extends string, Fallback = unknown> = typeof globalThis extends {
  [k in Name]: { prototype: infer T };
}
  ? T
  : Fallback;
