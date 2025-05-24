'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createAttributeWithCacheKey = void 0;
class AttributeWithCacheKeyImpl {
  constructor(attribute) {
    Object.assign(this, attribute);
  }
  get cacheKey() {
    if (!this.key) {
      this.key = Object.getOwnPropertyNames(this)
        .sort()
        .map((name) => `${this[name]}`)
        .join(';');
    }
    return this.key;
  }
}
const createAttributeWithCacheKey = (attribute) => new AttributeWithCacheKeyImpl(attribute);
exports.createAttributeWithCacheKey = createAttributeWithCacheKey;
//# sourceMappingURL=attribute-with-cache-key.js.map
