import { ono } from "@jsdevtools/ono";
import { resolve } from "../util/url.js";
import { ResolverError } from "../util/errors.js";
import type { FileInfo } from "../types/index.js";

export const sendRequest = async ({
  fetchOptions,
  redirects = [],
  timeout = 60_000,
  url,
}: {
  fetchOptions?: RequestInit;
  redirects?: string[];
  timeout?: number;
  url: URL | string;
}): Promise<{
  fetchOptions?: RequestInit;
  response: Response;
}> => {
  url = new URL(url);
  redirects.push(url.href);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, timeout);
  const response = await fetch(url, {
    signal: controller.signal,
    ...fetchOptions,
  });
  clearTimeout(timeoutId);

  if (response.status >= 300 && response.status <= 399) {
    if (redirects.length > 5) {
      throw new ResolverError(
        ono(
          { status: response.status },
          `Error requesting ${redirects[0]}. \nToo many redirects: \n  ${redirects.join(" \n  ")}`,
        ),
      );
    }

    if (!("location" in response.headers) || !response.headers.location) {
      throw ono({ status: response.status }, `HTTP ${response.status} redirect with no location header`);
    }

    return sendRequest({
      fetchOptions,
      redirects,
      timeout,
      url: resolve(url.href, response.headers.location as string),
    });
  }

  return { fetchOptions, response };
}

export const urlResolver = {
  handler: async ({
    arrayBuffer,
    fetch: _fetch,
    file,
  }: {
    arrayBuffer?: ArrayBuffer;
    fetch?: RequestInit;
    file: FileInfo;
  }): Promise<void> => {
    let data = arrayBuffer;

    if (!data) {
      try {
        const { fetchOptions, response } = await sendRequest({
          fetchOptions: {
            method: 'GET',
            ..._fetch,
          },
          url: file.url,
        });

        if (response.status >= 400) {
          // gracefully handle HEAD method not allowed
          if (response.status !== 405 || fetchOptions?.method !== 'HEAD') {
            throw ono({ status: response.status }, `HTTP ERROR ${response.status}`);
          }

          data = response.body ? await response.arrayBuffer() : new ArrayBuffer(0)
        }
      } catch (error: any) {
        throw new ResolverError(ono(error, `Error requesting ${file.url}`), file.url);
      }
    }

    file.data = Buffer.from(data!);
  },
};
