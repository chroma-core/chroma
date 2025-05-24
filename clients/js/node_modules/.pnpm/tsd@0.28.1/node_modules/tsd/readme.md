# tsd ![CI](https://github.com/SamVerschueren/tsd/workflows/CI/badge.svg)

> Check TypeScript type definitions

## Install

```sh
npm install --save-dev tsd
```

## Overview

This tool lets you write tests for your type definitions (i.e. your `.d.ts` files) by creating files with the `.test-d.ts` extension.

These `.test-d.ts` files will not be executed, and not even compiled in the standard way. Instead, these files will be parsed for special constructs such as `expectError<Foo>(bar)` and then statically analyzed against your type definitions.

The `tsd` CLI will search for the main `.d.ts` file in the current or specified directory, and test it with any `.test-d.ts` files in either the same directory or a test sub-directory (default: `test-d`):

```sh
[npx] tsd [path]
```

Use `tsd --help` for usage information. See [Order of Operations](#order-of-operations) for more details on how `tsd` finds and executes tests.

*Note: the CLI is primarily used to test an entire project, not a specific file. For more specific configuration and advanced usage, see [Configuration](#configuration) and [Programmatic API](#programmatic-api).*

## Usage

Let's assume we wrote a `index.d.ts` type definition for our concat module.

```ts
declare const concat: {
	(value1: string, value2: string): string;
	(value1: number, value2: number): string;
};

export default concat;
```

In order to test this definition, add a `index.test-d.ts` file.

```ts
import concat from '.';

concat('foo', 'bar');
concat(1, 2);
```

Running `npx tsd` as a command will verify that the type definition works correctly.

Let's add some extra [assertions](#assertions). We can assert the return type of our function call to match a certain type.

```ts
import {expectType} from 'tsd';
import concat from '.';

expectType<string>(concat('foo', 'bar'));
expectType<string>(concat(1, 2));
```

The `tsd` command will succeed again.

We change our implementation and type definition to return a `number` when both inputs are of type `number`.

```ts
declare const concat: {
	(value1: string, value2: string): string;
	(value1: number, value2: number): number;
};

export default concat;
```

If we don't change the test file and we run the `tsd` command again, the test will fail.

<img src="media/screenshot.png" width="1330">

### Strict type assertions

Type assertions are strict. This means that if you expect the type to be `string | number` but the argument is of type `string`, the tests will fail.

```ts
import {expectType} from 'tsd';
import concat from '.';

expectType<string>(concat('foo', 'bar'));
expectType<string | number>(concat('foo', 'bar'));
```

If we run `tsd`, we will notice that it reports an error because the `concat` method returns the type `string` and not `string | number`.

<img src="media/strict-assert.png" width="1330">

If you still want loose type assertion, you can use `expectAssignable` for that.

```ts
import {expectType, expectAssignable} from 'tsd';
import concat from '.';

expectType<string>(concat('foo', 'bar'));
expectAssignable<string | number>(concat('foo', 'bar'));
```

### Top-level `await`

If your method returns a `Promise`, you can use top-level `await` to resolve the value instead of wrapping it in an `async` [IIFE](https://developer.mozilla.org/en-US/docs/Glossary/IIFE).

```ts
import {expectType, expectError} from 'tsd';
import concat from '.';

expectType<Promise<string>>(concat('foo', 'bar'));

expectType<string>(await concat('foo', 'bar'));

expectError(await concat(true, false));
```

## Order of Operations

When searching for `.test-d.ts` files and executing them, `tsd` does the following:

1. Locates the project's `package.json`, which needs to be in the current or specified directory (e.g. `/path/to/project` or `process.cwd()`). Fails if none is found.

2. Finds a `.d.ts` file, checking to see if one was specified manually or in the `types` field of the `package.json`. If neither is found, attempts to find one in the project directory named the same as the `main` field of the `package.json` or `index.d.ts`. Fails if no `.d.ts` file is found.

3. Finds `.test-d.ts` and `.test-d.tsx` files, which can either be in the project's root directory, a [specific folder](#test-directory) (by default `/[project-root]/test-d`), or specified individually [programatically](#testfiles) or via [the CLI](#via-the-cli). Fails if no test files are found.

4. Runs the `.test-d.ts` files through the TypeScript compiler and statically analyzes them for errors.

5. Checks the errors against [assertions](#assertions) and reports any mismatches.

## Assertions

### expectType&lt;T&gt;(expression: T)

Asserts that the type of `expression` is identical to type `T`.

### expectNotType&lt;T&gt;(expression: any)

Asserts that the type of `expression` is not identical to type `T`.

### expectAssignable&lt;T&gt;(expression: T)

Asserts that the type of `expression` is assignable to type `T`.

### expectNotAssignable&lt;T&gt;(expression: any)

Asserts that the type of `expression` is not assignable to type `T`.

### expectError&lt;T = any&gt;(expression: T)

Asserts that `expression` throws an error. Will not ignore syntax errors.

### expectDeprecated(expression: any)

Asserts that `expression` is marked as [`@deprecated`](https://jsdoc.app/tags-deprecated.html).

### expectNotDeprecated(expression: any)

Asserts that `expression` is not marked as [`@deprecated`](https://jsdoc.app/tags-deprecated.html).

### printType(expression: any)

Prints the type of `expression` as a warning.

Useful if you don't know the exact type of the expression passed to `printType()` or the type is too complex to write out by hand.

### expectNever(expression: never)

Asserts that the type and return type of `expression` is `never`.

Useful for checking that all branches are covered.

### expectDocCommentIncludes&lt;T&gt;(expression: any)

Asserts that the documentation comment of `expression` includes string literal type `T`.

## Configuration

`tsd` is designed to be used with as little configuration as possible. However, if you need a bit more control, a project's `package.json` and the `tsd` CLI offer a limited set of configurations.

For more advanced use cases (such as integrating `tsd` with testing frameworks), see [Programmatic API](#programmatic-api).

### Via `package.json`

`tsd` uses a project's `package.json` to find types and test files as well as for some configuration. It must exist in the path given to `tsd`.

For more information on how `tsd` finds a `package.json`, see [Order of Operations](#order-of-operations).

#### Test Directory

When you have spread your tests over multiple files, you can store all those files in a test directory called `test-d`. If you want to use another directory name, you can change it in your project's `package.json`:

```json
{
	"name": "my-module",
	"tsd": {
		"directory": "my-test-dir"
	}
}
```

Now you can put all your test files in the `my-test-dir` directory.

#### Custom TypeScript Config

By default, `tsd` applies the following configuration:

```json5
{
	"strict": true,
	"jsx": "react",
	"target": "es2020",
	"lib": [
		"es2020",
		"dom",
		"dom.iterable"
	],
	"module": "commonjs",
	"esModuleInterop": true,
	"noUnusedLocals": false,
	// The following options are set and are not overridable.
	// Set to `nodenext` if `module` is `nodenext`, `node16` if `module` is `node16` or `node` otherwise.
	"moduleResolution": "node" | "node16" | "nodenext",
	"skipLibCheck": false
}
```

These options will be overridden if a `tsconfig.json` file is found in your project. You also have the possibility to provide a custom config by specifying it in `package.json`:

```json
{
	"name": "my-module",
	"tsd": {
		"compilerOptions": {
			"strict": false
		}
	}
}
```

*Default options will apply if you don't override them explicitly. You can't override the `moduleResolution` or `skipLibCheck` options.*

### Via the CLI

The `tsd` CLI is designed to test a whole project at once, and as such only offers a couple of flags for configuration.

#### --typings

Alias: `-t`

Path to the type definition file you want to test. Same as [`typingsFile`](#typingsfile).

#### --files

Alias: `-f`

An array of test files with their path. Same as [`testFiles`](#testfiles).

## Programmatic API

You can use the programmatic API to retrieve the diagnostics and do something with them. This can be useful to run the tests with AVA, Jest or any other testing framework.

```ts
import tsd from 'tsd';

const diagnostics = await tsd();

console.log(diagnostics.length);
//=> 2
```

You can also make use of the CLI's formatter to generate the same formatting output when running `tsd` programmatically.

```ts
import tsd, {formatter} from 'tsd';

const formattedDiagnostics = formatter(await tsd());
```

### tsd(options?)

Retrieve the type definition diagnostics of the project.

#### options

Type: `object`

##### cwd

Type: `string`\
Default: `process.cwd()`

Current working directory of the project to retrieve the diagnostics for.

##### typingsFile

Type: `string`\
Default: The `types` property in `package.json`.

Path to the type definition file you want to test. This can be useful when using a test runner to test specific type definitions per test.

##### testFiles

Type: `string[]`\
Default: Finds files with `.test-d.ts` or `.test-d.tsx` extension.

An array of test files with their path. Uses [globby](https://github.com/sindresorhus/globby) under the hood so that you can fine tune test file discovery.
