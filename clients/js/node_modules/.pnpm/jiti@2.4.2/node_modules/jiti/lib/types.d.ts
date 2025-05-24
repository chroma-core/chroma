export declare function createJiti(id: string, userOptions?: JitiOptions): Jiti;

/**
 * Jiti instance
 *
 * Calling jiti() is similar to CommonJS require() but adds extra features such as Typescript and ESM compatibility.
 *
 * **Note:**It is recommended to use `await jiti.import` instead
 */
export interface Jiti extends NodeRequire {
  /**
   * Resolved options
   */
  options: JitiOptions;

  /**
   * ESM import a module with additional Typescript and ESM compatibility.
   *
   * If you need the default export of module, you can use `jiti.import(id, { default: true })` as shortcut to `mod?.default ?? mod`.
   */
  import<T = unknown>(
    id: string,
    opts?: JitiResolveOptions & { default?: true },
  ): Promise<T>;

  /**
   * Resolve with ESM import conditions.
   */
  esmResolve(id: string, parentURL?: string): string;
  esmResolve<T extends JitiResolveOptions = JitiResolveOptions>(
    id: string,
    opts?: T,
  ): T["try"] extends true ? string | undefined : string;

  /**
   * Transform source code
   */
  transform: (opts: TransformOptions) => string;

  /**
   * Evaluate transformed code as a module
   */
  evalModule: (source: string, options?: EvalModuleOptions) => unknown;
}

/**
 * Jiti instance options
 */
export interface JitiOptions {
  /**
   * Filesystem source cache (enabled by default)
   *
   * An string can be passed to set the custom cache directory.
   *
   * By default (when is `true`), jiti uses  `node_modules/.cache/jiti` (if exists) or `{TMP_DIR}/jiti`.
   *
   * This option can also be disabled using `JITI_FS_CACHE=false` environment variable.
   *
   * **Note:** It is recommended to keep this option enabled for better performance.
   */
  fsCache?: boolean | string;

  /** @deprecated Use `fsCache` option. */
  cache?: boolean | string;

  /**
   * Runtime module cache (enabled by default)
   *
   * Disabling allows editing code and importing same module multiple times.
   *
   * When enabled, jiti integrates with Node.js native CommonJS cache store.
   *
   * This option can also be disabled using `JITI_MODULE_CACHE=false` environment variable.
   */
  moduleCache?: boolean;

  /** @deprecated Use `moduleCache` option.  */
  requireCache?: boolean;

  /**
   * Custom transform function
   */
  transform?: (opts: TransformOptions) => TransformResult;

  /**
   * Enable verbose debugging (disabled by default).
   *
   * Can also be enabled using `JITI_DEBUG=1` environment variable.
   */
  debug?: boolean;

  /**
   * Enable sourcemaps (enabled by default)
   *
   * Can also be disabled using `JITI_SOURCE_MAPS=0` environment variable.
   */
  sourceMaps?: boolean;

  /**
   * Jiti combines module exports with the `default` export using an internal Proxy to improve compatibility with mixed CJS/ESM usage. You can check the current implementation [here](https://github.com/unjs/jiti/blob/main/src/utils.ts#L105).
   *
   * Can be disabled using `JITI_INTEROP_DEFAULT=0` environment variable.
   */
  interopDefault?: boolean;

  /**
   * Jiti hard source cache version (internal)
   */
  cacheVersion?: string;

  /**
   * Supported extensions to resolve.
   *
   * Default `[".js", ".mjs", ".cjs", ".ts", ".mts", ".cts", ".json"]`
   */
  extensions?: string[];

  /**
   * Transform options
   */
  transformOptions?: Omit<TransformOptions, "source">;

  /**
   * Resolve aliases
   *
   * You can use `JITI_ALIAS` environment variable to set aliases as a JSON string.
   */
  alias?: Record<string, string>;

  /**
   * List of modules (within `node_modules`) to always use native require/import for them.
   *
   * You can use `JITI_NATIVE_MODULES` environment variable to set native modules as a JSON string.
   *
   */
  nativeModules?: string[];

  /**
   * List of modules (within `node_modules`) to transform them regardless of syntax.
   *
   * You can use `JITI_TRANSFORM_MODULES` environment variable to set transform modules as a JSON string.
   */
  transformModules?: string[];

  /**
   * Parent module's import.meta context to use for ESM resolution.
   *
   * (Only used for `jiti/native` import)
   */
  importMeta?: ImportMeta;

  /**
   * Try to use native require and import without jiti transformations first.
   *
   * Enabled if Bun is detected.
   */
  tryNative?: boolean;

  /**
   * Enable JSX support Enable JSX support using [`@babel/plugin-transform-react-jsx`](https://babeljs.io/docs/babel-plugin-transform-react-jsx).
   *
   * @default false
   *
   * You can also use `JITI_JSX=1` environment variable to enable JSX support.
   */
  jsx?: boolean | JSXOptions;
}

interface NodeRequire {
  /**
   * Module cache
   */
  cache: ModuleCache;

  /** @deprecated Prefer `await jiti.import()` for better compatibility. */
  (id: string): any;

  /** @deprecated Prefer `jiti.esmResolve` for better compatibility. */
  resolve: {
    /** @deprecated */
    (id: string, options?: { paths?: string[] | undefined }): string;
    /** @deprecated */
    paths(request: string): string[] | null;
  };

  /** @deprecated CommonJS API */
  extensions: Record<
    ".js" | ".json" | ".node",
    (m: NodeModule, filename: string) => any | undefined
  >;

  /** @deprecated CommonJS API */
  main: NodeModule | undefined;
}

export interface NodeModule {
  /**
   * `true` if the module is running during the Node.js preload
   */
  isPreloading: boolean;
  exports: any;
  require: NodeRequire;
  id: string;
  filename: string;
  loaded: boolean;
  /** @deprecated since v14.6.0 Please use `require.main` and `module.children` instead. */
  parent: NodeModule | null | undefined;
  children: NodeModule[];
  /**
   * @since v11.14.0
   *
   * The directory name of the module. This is usually the same as the path.dirname() of the module.id.
   */
  path: string;
  paths: string[];
}

export type ModuleCache = Record<string, NodeModule>;

export type EvalModuleOptions = Partial<{
  id: string;
  filename: string;
  ext: string;
  cache: ModuleCache;
  async: boolean;
  forceTranspile: boolean;
}>;

export interface TransformOptions {
  source: string;
  filename?: string;
  ts?: boolean;
  retainLines?: boolean;
  interopDefault?: boolean;
  async?: boolean;
  jsx?: boolean | JSXOptions;
  babel?: Record<string, any>;
}

export interface TransformResult {
  code: string;
  error?: any;
}

export interface JitiResolveOptions {
  conditions?: string[];
  parentURL?: string | URL;
  try?: boolean;
}

/** Reference: https://babeljs.io/docs/babel-plugin-transform-react-jsx#options */
export interface JSXOptions {
  throwIfNamespace?: boolean;
  runtime?: "classic" | "automatic";
  importSource?: string;
  pragma?: string;
  pragmaFrag?: string;
  useBuiltIns?: boolean;
  useSpread?: boolean;
}
