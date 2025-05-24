// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

class AttributeWithCacheKeyImpl {
  constructor(attribute: Record<string, unknown>) {
    Object.assign(this, attribute);
  }

  private key: string;
  public get cacheKey(): string {
    if (!this.key) {
      this.key = Object.getOwnPropertyNames(this)
        .sort()
        .map((name) => `${(this as Record<string, unknown>)[name]}`)
        .join(';');
    }
    return this.key;
  }
}

export interface AttributeWithCacheKey {
  readonly cacheKey: string;
}

export const createAttributeWithCacheKey = <T extends Record<string, unknown>>(
  attribute: T,
): T & AttributeWithCacheKey => new AttributeWithCacheKeyImpl(attribute) as unknown as T & AttributeWithCacheKey;
