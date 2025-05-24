import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { $RefParser } from '..';

describe('bundle', () => {
  it('handles circular reference with description', async () => {
    const refParser = new $RefParser();
    const pathOrUrlOrSchema = path.resolve('lib', '__tests__', 'spec', 'circular-ref-with-description.json');
    const schema = await refParser.bundle({ pathOrUrlOrSchema });
    expect(schema).not.toBeUndefined();
  });
});
