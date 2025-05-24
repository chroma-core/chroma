import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { getResolvedInput } from '../index';

describe('getResolvedInput', () => {
  it('handles url', async () => {
    const pathOrUrlOrSchema =  'https://foo.com';
    const resolvedInput = await getResolvedInput({ pathOrUrlOrSchema });
    expect(resolvedInput.type).toBe('url');
    expect(resolvedInput.schema).toBeUndefined();
    expect(resolvedInput.path).toBe('https://foo.com/');
  });

  it('handles file', async () => {
    const pathOrUrlOrSchema =  './path/to/openapi.json';
    const resolvedInput = await getResolvedInput({ pathOrUrlOrSchema });
    expect(resolvedInput.type).toBe('file');
    expect(resolvedInput.schema).toBeUndefined();
    expect(resolvedInput.path).toBe(path.resolve('./path/to/openapi.json'));
  });

  it('handles raw spec', async () => {
    const pathOrUrlOrSchema =  {
      info: {
        version: '1.0.0',
      },
      openapi: '3.1.0',
      paths: {},
    };
    const resolvedInput = await getResolvedInput({ pathOrUrlOrSchema });
    expect(resolvedInput.type).toBe('json');
    expect(resolvedInput.schema).toEqual({
      info: {
        version: '1.0.0',
      },
      openapi: '3.1.0',
      paths: {},
    });
    expect(resolvedInput.path).toBe('');
  });
});
