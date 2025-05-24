import type { Auth } from '@hey-api/client-core';
import { describe, expect, it, vi } from 'vitest';

import { getParseAs, setAuthParams } from '../utils';

describe('getParseAs', () => {
  const scenarios: Array<{
    content: Parameters<typeof getParseAs>[0];
    parseAs: ReturnType<typeof getParseAs>;
  }> = [
    {
      content: null,
      parseAs: 'stream',
    },
    {
      content: 'application/json',
      parseAs: 'json',
    },
    {
      content: 'application/ld+json',
      parseAs: 'json',
    },
    {
      content: 'application/ld+json;charset=utf-8',
      parseAs: 'json',
    },
    {
      content: 'application/ld+json; charset=utf-8',
      parseAs: 'json',
    },
    {
      content: 'multipart/form-data',
      parseAs: 'formData',
    },
    {
      content: 'application/*',
      parseAs: 'blob',
    },
    {
      content: 'audio/*',
      parseAs: 'blob',
    },
    {
      content: 'image/*',
      parseAs: 'blob',
    },
    {
      content: 'video/*',
      parseAs: 'blob',
    },
    {
      content: 'text/*',
      parseAs: 'text',
    },
    {
      content: 'unsupported',
      parseAs: undefined,
    },
  ];

  it.each(scenarios)(
    'detects $content as $parseAs',
    async ({ content, parseAs }) => {
      expect(getParseAs(content)).toEqual(parseAs);
    },
  );
});

describe('setAuthParams', () => {
  it('sets bearer token in headers', async () => {
    const auth = vi.fn().mockReturnValue('foo');
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          name: 'baz',
          scheme: 'bearer',
          type: 'http',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('baz')).toBe('Bearer foo');
    expect(Object.keys(query).length).toBe(0);
  });

  it('sets access token in query', async () => {
    const auth = vi.fn().mockReturnValue('foo');
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          in: 'query',
          name: 'baz',
          scheme: 'bearer',
          type: 'http',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('baz')).toBeNull();
    expect(query.baz).toBe('Bearer foo');
  });

  it('sets Authorization header when `in` and `name` are undefined', async () => {
    const auth = vi.fn().mockReturnValue('foo');
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          type: 'http',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('Authorization')).toBe('foo');
    expect(query).toEqual({});
  });

  it('sets first scheme only', async () => {
    const auth = vi.fn().mockReturnValue('foo');
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          name: 'baz',
          scheme: 'bearer',
          type: 'http',
        },
        {
          in: 'query',
          name: 'baz',
          scheme: 'bearer',
          type: 'http',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('baz')).toBe('Bearer foo');
    expect(Object.keys(query).length).toBe(0);
  });

  it('sets first scheme with token', async () => {
    const auth = vi.fn().mockImplementation((auth: Auth) => {
      if (auth.type === 'apiKey') {
        return;
      }
      return 'foo';
    });
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          name: 'baz',
          type: 'apiKey',
        },
        {
          in: 'query',
          name: 'baz',
          scheme: 'bearer',
          type: 'http',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('baz')).toBeNull();
    expect(query.baz).toBe('Bearer foo');
  });

  it('sets an API key in a cookie', async () => {
    const auth = vi.fn().mockReturnValue('foo');
    const headers = new Headers();
    const query: Record<any, unknown> = {};
    await setAuthParams({
      auth,
      headers,
      query,
      security: [
        {
          in: 'cookie',
          name: 'baz',
          type: 'apiKey',
        },
      ],
    });
    expect(auth).toHaveBeenCalled();
    expect(headers.get('Cookie')).toBe('baz=foo');
    expect(query).toEqual({});
  });
});
