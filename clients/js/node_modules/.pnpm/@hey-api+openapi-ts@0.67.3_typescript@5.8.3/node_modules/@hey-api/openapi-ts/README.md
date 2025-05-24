<div align="center">
  <img alt="Hey API logo" height="150" src="https://heyapi.dev/images/logo-300w.png" width="150">
  <h1 align="center"><b>OpenAPI TypeScript</b></h1>
  <p align="center">🚀 The OpenAPI to TypeScript codegen. Generate clients, SDKs, validators, and more.</p>
</div>

<br/>

<p align="center">
  <a href="https://opensource.org/license/mit" rel="nofollow"><img src="https://img.shields.io/github/license/hey-api/openapi-ts" alt="MIT License"></a>
  <a href="https://github.com/hey-api/openapi-ts/actions?query=branch%3Amain"><img src="https://img.shields.io/github/last-commit/hey-api/openapi-ts" alt="Last commit" /></a>
  <a href="https://github.com/hey-api/openapi-ts/actions?query=branch%3Amain"><img src="https://github.com/hey-api/openapi-ts/actions/workflows/ci.yml/badge.svg?event=push&branch=main" alt="CI status" /></a>
  <a href="https://github.com/hey-api/openapi-ts/issues" rel="nofollow"><img src="https://img.shields.io/github/issues/hey-api/openapi-ts" alt="Number of open issues"></a>
  <a href="https://app.codecov.io/gh/hey-api/openapi-ts/tree/main"><img src="https://codecov.io/gh/hey-api/openapi-ts/branch/main/graph/badge.svg" alt="Test coverage" /></a>
</p>

<p align="center">
  <a href="https://stackblitz.com/edit/hey-api-example?file=openapi-ts.config.ts,src%2Fclient%2Fschemas.gen.ts,src%2Fclient%2Fsdk.gen.ts,src%2Fclient%2Ftypes.gen.ts">Demo</a>
  <span>&nbsp;•&nbsp;</span>
  <a href="https://heyapi.dev">Documentation</a>
  <span>&nbsp;•&nbsp;</span>
  <a href="https://github.com/hey-api/openapi-ts/issues">Issues</a>
  <span>&nbsp;•&nbsp;</span>
  <a href="https://github.com/orgs/hey-api/discussions/1495">Roadmap</a>
  <span>&nbsp;•&nbsp;</span>
  <a href="https://npmjs.com/package/@hey-api/openapi-ts">npm</a>
</p>

<br/>

## Features

- runs in CLI, Node.js 18+, or npx
- works with OpenAPI 2.0, 3.0, and 3.1
- customizable types and SDKs
- clients for your runtime (Fetch API, Axios, Next.js, Nuxt, etc.)
- plugin ecosystem to reduce third-party boilerplate
- custom plugins and custom clients
- [integration](https://heyapi.dev/openapi-ts/integrations) with Hey API platform

## Dashboard

Hey API is an ecosystem of products helping you build better APIs. Superpower your codegen and APIs with our platform.

[Sign In](https://app.heyapi.dev) to Hey API platform.

## Sponsors

Love Hey API? Become our [sponsor](https://github.com/sponsors/hey-api).

<p>
  <a href="https://kutt.it/pkEZyc" target="_blank">
    <img alt="Stainless logo" height="50" src="https://heyapi.dev/images/stainless-logo-wordmark-480w.jpeg" />
  </a>
</p>

## Quick Start

The fastest way to use `@hey-api/openapi-ts` is via npx

```sh
npx @hey-api/openapi-ts \
  -i https://get.heyapi.dev/hey-api/backend \
  -o src/client \
  -c @hey-api/client-fetch
```

Congratulations on creating your first client! 🎉 You can learn more about the generated files on the [Output](https://heyapi.dev/openapi-ts/output) page.

Before you can make API requests with the client you've just created, you need to install `@hey-api/client-fetch` and configure it.

## Installation

#### npm

```sh
npm install @hey-api/client-fetch && npm install @hey-api/openapi-ts -D
```

#### pnpm

```sh
pnpm add @hey-api/client-fetch && pnpm add @hey-api/openapi-ts -D
```

#### yarn

```sh
yarn add @hey-api/client-fetch && yarn add @hey-api/openapi-ts -D
```

#### bun

```sh
bun add @hey-api/client-fetch && bun add @hey-api/openapi-ts -D
```

We recommend pinning an exact version so you can safely upgrade when you're ready. This package is in [initial development](https://semver.org/spec/v0.1.0.html#spec-item-5) and its API might change before v1.

### CLI

Most people run `@hey-api/openapi-ts` via CLI. To do that, add a script to your `package.json` file which will make `openapi-ts` executable through script.

```json
"scripts": {
  "openapi-ts": "openapi-ts"
}
```

The above script can be executed by running `npm run openapi-ts` or equivalent command in other package managers. Next, we need to create a [configuration](https://heyapi.dev/openapi-ts/configuration) file and move our options from Quick Start to it.

### Node.js

You can also generate clients programmatically by importing `@hey-api/openapi-ts` in a TypeScript file.

```ts
import { createClient } from '@hey-api/openapi-ts';

createClient({
  input: 'https://get.heyapi.dev/hey-api/backend',
  output: 'src/client',
  plugins: ['@hey-api/client-fetch'],
});
```

## Configuration

`@hey-api/openapi-ts` supports loading configuration from any file inside your project root folder supported by [jiti loader](https://github.com/unjs/c12?tab=readme-ov-file#-features). Below are the most common file formats.

#### `openapi-ts.config.ts`

```js
import { defineConfig } from '@hey-api/openapi-ts';

export default defineConfig({
  input: 'https://get.heyapi.dev/hey-api/backend',
  output: 'src/client',
  plugins: ['@hey-api/client-fetch'],
});
```

#### `openapi-ts.config.cjs`

```js
/** @type {import('@hey-api/openapi-ts').UserConfig} */
module.exports = {
  input: 'https://get.heyapi.dev/hey-api/backend',
  output: 'src/client',
  plugins: ['@hey-api/client-fetch'],
};
```

#### `openapi-ts.config.mjs`

```js
/** @type {import('@hey-api/openapi-ts').UserConfig} */
export default {
  input: 'https://get.heyapi.dev/hey-api/backend',
  output: 'src/client',
  plugins: ['@hey-api/client-fetch'],
};
```

Alternatively, you can use `openapi-ts.config.js` and configure the export statement depending on your project setup.

### Input

Input is the first thing you must define. It can be a path, URL, or a string content resolving to an OpenAPI specification. Hey API supports all valid OpenAPI versions and file formats.

> If you use an HTTPS URL with a self-signed certificate in development, you will need to set [`NODE_TLS_REJECT_UNAUTHORIZED=0`](https://github.com/hey-api/openapi-ts/issues/276#issuecomment-2043143501) in your environment.

### Output

Output is the next thing to define. It can be either a string pointing to the destination folder or a configuration object containing the destination folder path and optional settings (these are described below).

> You should treat the output folder as a dependency. Do not directly modify its contents as your changes might be erased when you run `@hey-api/openapi-ts` again.

### Client

Clients are responsible for sending the actual HTTP requests. Using clients is not required, but you must add a client to `plugins` if you're generating SDKs (enabled by default).

### Native Clients

- [`@hey-api/client-fetch`](https://heyapi.dev/openapi-ts/clients/fetch)
- [`@hey-api/client-axios`](https://heyapi.dev/openapi-ts/clients/axios)
- [`@hey-api/client-next`](https://heyapi.dev/openapi-ts/clients/next-js)
- [`@hey-api/client-nuxt`](https://heyapi.dev/openapi-ts/clients/nuxt)

Don't see your client? [Build your own](https://heyapi.dev/openapi-ts/clients/custom) or let us know your interest by [opening an issue](https://github.com/hey-api/openapi-ts/issues).

## Plugins

Plugins are responsible for generating artifacts from your input. By default, Hey API will generate TypeScript interfaces and SDK from your OpenAPI specification. You can add, remove, or customize any of the plugins. In fact, we highly encourage you to do so!

### Native Plugins

These plugins help reduce boilerplate associated with third-party dependencies. Hey API natively supports the most popular packages. Please open an issue on [GitHub](https://github.com/hey-api/openapi-ts/issues) if you'd like us to support your favorite package.

- [`@hey-api/schemas`](https://heyapi.dev/openapi-ts/output/json-schema)
- [`@hey-api/sdk`](https://heyapi.dev/openapi-ts/output/sdk)
- [`@hey-api/transformers`](https://heyapi.dev/openapi-ts/transformers)
- [`@hey-api/typescript`](https://heyapi.dev/openapi-ts/output/typescript)
- [`@tanstack/angular-query-experimental`](https://heyapi.dev/openapi-ts/plugins/tanstack-query)
- [`@tanstack/react-query`](https://heyapi.dev/openapi-ts/plugins/tanstack-query)
- [`@tanstack/solid-query`](https://heyapi.dev/openapi-ts/plugins/tanstack-query)
- [`@tanstack/svelte-query`](https://heyapi.dev/openapi-ts/plugins/tanstack-query)
- [`@tanstack/vue-query`](https://heyapi.dev/openapi-ts/plugins/tanstack-query)
- [`fastify`](https://heyapi.dev/openapi-ts/plugins/fastify)
- [`zod`](https://heyapi.dev/openapi-ts/plugins/zod)

### Planned Plugins

The following plugins are planned but not in development yet. You can help us prioritize them by voting on [GitHub](https://github.com/hey-api/openapi-ts/labels/RSVP%20%F0%9F%91%8D%F0%9F%91%8E).

- [Ajv](https://heyapi.dev/openapi-ts/plugins/ajv)
- [Arktype](https://heyapi.dev/openapi-ts/plugins/arktype)
- [Express](https://heyapi.dev/openapi-ts/plugins/express)
- [Faker](https://heyapi.dev/openapi-ts/plugins/faker)
- [Hono](https://heyapi.dev/openapi-ts/plugins/hono)
- [Joi](https://heyapi.dev/openapi-ts/plugins/joi)
- [Koa](https://heyapi.dev/openapi-ts/plugins/koa)
- [MSW](https://heyapi.dev/openapi-ts/plugins/msw)
- [Nest](https://heyapi.dev/openapi-ts/plugins/nest)
- [Nock](https://heyapi.dev/openapi-ts/plugins/nock)
- [Pinia Colada](https://heyapi.dev/openapi-ts/plugins/pinia-colada)
- [Superstruct](https://heyapi.dev/openapi-ts/plugins/superstruct)
- [Supertest](https://heyapi.dev/openapi-ts/plugins/supertest)
- [SWR](https://heyapi.dev/openapi-ts/plugins/swr)
- [TypeBox](https://heyapi.dev/openapi-ts/plugins/typebox)
- [Valibot](https://heyapi.dev/openapi-ts/plugins/valibot)
- [Yup](https://heyapi.dev/openapi-ts/plugins/yup)
- [Zustand](https://heyapi.dev/openapi-ts/plugins/zustand)

Don't see your plugin? [Build your own](https://heyapi.dev/openapi-ts/plugins/custom) or let us know your interest by [opening an issue](https://github.com/hey-api/openapi-ts/issues).

## Migration Guides

[OpenAPI Typescript Codegen](https://heyapi.dev/openapi-ts/migrating#openapi-typescript-codegen)

## License

Released under the [MIT License](https://github.com/hey-api/openapi-ts/blob/main/LICENSE.md).
