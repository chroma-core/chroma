// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Env } from 'onnxruntime-common';

import { WebGLContext } from './backends/webgl/webgl-context';

export declare namespace Logger {
  export interface SeverityTypeMap {
    verbose: 'v';
    info: 'i';
    warning: 'w';
    error: 'e';
    fatal: 'f';
  }

  export type Severity = keyof SeverityTypeMap;

  export type Provider = 'none' | 'console';

  /**
   * Logging config that used to control the behavior of logger
   */
  export interface Config {
    /**
     * Specify the logging provider. 'console' by default
     */
    provider?: Provider;
    /**
     * Specify the minimal logger serverity. 'warning' by default
     */
    minimalSeverity?: Logger.Severity;
    /**
     * Whether to output date time in log. true by default
     */
    logDateTime?: boolean;
    /**
     * Whether to output source information (Not yet supported). false by default
     */
    logSourceLocation?: boolean;
  }

  export interface CategorizedLogger {
    verbose(content: string): void;
    info(content: string): void;
    warning(content: string): void;
    error(content: string): void;
    fatal(content: string): void;
  }
}

// eslint-disable-next-line @typescript-eslint/no-redeclare
export interface Logger {
  (category: string): Logger.CategorizedLogger;

  verbose(content: string): void;
  verbose(category: string, content: string): void;
  info(content: string): void;
  info(category: string, content: string): void;
  warning(content: string): void;
  warning(category: string, content: string): void;
  error(content: string): void;
  error(category: string, content: string): void;
  fatal(content: string): void;
  fatal(category: string, content: string): void;

  /**
   * Reset the logger configuration.
   * @param config specify an optional default config
   */
  reset(config?: Logger.Config): void;
  /**
   * Set the logger's behavior on the given category
   * @param category specify a category string. If '*' is specified, all previous configuration will be overwritten. If
   * '' is specified, the default behavior will be updated.
   * @param config the config object to indicate the logger's behavior
   */
  set(category: string, config: Logger.Config): void;

  /**
   * Set the logger's behavior from ort-common env
   * @param env the env used to set logger. Currently only setting loglevel is supported through Env.
   */
  setWithEnv(env: Env): void;
}

interface LoggerProvider {
  log(severity: Logger.Severity, content: string, category?: string): void;
}
class NoOpLoggerProvider implements LoggerProvider {
  log(_severity: Logger.Severity, _content: string, _category?: string) {
    // do nothing
  }
}
class ConsoleLoggerProvider implements LoggerProvider {
  log(severity: Logger.Severity, content: string, category?: string) {
    // eslint-disable-next-line no-console
    console.log(`${this.color(severity)} ${category ? '\x1b[35m' + category + '\x1b[0m ' : ''}${content}`);
  }

  private color(severity: Logger.Severity) {
    switch (severity) {
      case 'verbose':
        return '\x1b[34;40mv\x1b[0m';
      case 'info':
        return '\x1b[32mi\x1b[0m';
      case 'warning':
        return '\x1b[30;43mw\x1b[0m';
      case 'error':
        return '\x1b[31;40me\x1b[0m';
      case 'fatal':
        return '\x1b[101mf\x1b[0m';
      default:
        throw new Error(`unsupported severity: ${severity}`);
    }
  }
}

const SEVERITY_VALUE = {
  verbose: 1000,
  info: 2000,
  warning: 4000,
  error: 5000,
  fatal: 6000,
};

const LOGGER_PROVIDER_MAP: { readonly [provider: string]: Readonly<LoggerProvider> } = {
  ['none']: new NoOpLoggerProvider(),
  ['console']: new ConsoleLoggerProvider(),
};
const LOGGER_DEFAULT_CONFIG = {
  provider: 'console',
  minimalSeverity: 'warning',
  logDateTime: true,
  logSourceLocation: false,
};
let LOGGER_CONFIG_MAP: { [category: string]: Readonly<Required<Logger.Config>> } = {
  ['']: LOGGER_DEFAULT_CONFIG as Required<Logger.Config>,
};

function log(category: string): Logger.CategorizedLogger;
function log(severity: Logger.Severity, content: string): void;
function log(severity: Logger.Severity, category: string, content: string): void;
function log(severity: Logger.Severity, arg1: string, arg2?: string): void;
function log(
  arg0: string | Logger.Severity,
  arg1?: string,
  arg2?: string | number,
  arg3?: number,
): Logger.CategorizedLogger | void {
  if (arg1 === undefined) {
    // log(category: string): Logger.CategorizedLogger;
    return createCategorizedLogger(arg0);
  } else if (arg2 === undefined) {
    // log(severity, content);
    logInternal(arg0 as Logger.Severity, arg1, 1);
  } else if (typeof arg2 === 'number' && arg3 === undefined) {
    // log(severity, content, stack)
    logInternal(arg0 as Logger.Severity, arg1, arg2);
  } else if (typeof arg2 === 'string' && arg3 === undefined) {
    // log(severity, category, content)
    logInternal(arg0 as Logger.Severity, arg2, 1, arg1);
  } else if (typeof arg2 === 'string' && typeof arg3 === 'number') {
    // log(severity, category, content, stack)
    logInternal(arg0 as Logger.Severity, arg2, arg3, arg1);
  } else {
    throw new TypeError('input is valid');
  }
}

function createCategorizedLogger(category: string): Logger.CategorizedLogger {
  return {
    verbose: log.verbose.bind(null, category),
    info: log.info.bind(null, category),
    warning: log.warning.bind(null, category),
    error: log.error.bind(null, category),
    fatal: log.fatal.bind(null, category),
  };
}

// NOTE: argument 'category' is put the last parameter beacause typescript
// doesn't allow optional argument put in front of required argument. This
// order is different from a usual logging API.
function logInternal(severity: Logger.Severity, content: string, _stack: number, category?: string) {
  const config = LOGGER_CONFIG_MAP[category || ''] || LOGGER_CONFIG_MAP[''];
  if (SEVERITY_VALUE[severity] < SEVERITY_VALUE[config.minimalSeverity]) {
    return;
  }

  if (config.logDateTime) {
    content = `${new Date().toISOString()}|${content}`;
  }

  if (config.logSourceLocation) {
    // TODO: calculate source location from 'stack'
  }

  LOGGER_PROVIDER_MAP[config.provider].log(severity, content, category);
}

// eslint-disable-next-line @typescript-eslint/no-namespace
namespace log {
  export function verbose(content: string): void;
  export function verbose(category: string, content: string): void;
  export function verbose(arg0: string, arg1?: string) {
    log('verbose', arg0, arg1);
  }
  export function info(content: string): void;
  export function info(category: string, content: string): void;
  export function info(arg0: string, arg1?: string) {
    log('info', arg0, arg1);
  }
  export function warning(content: string): void;
  export function warning(category: string, content: string): void;
  export function warning(arg0: string, arg1?: string) {
    log('warning', arg0, arg1);
  }
  export function error(content: string): void;
  export function error(category: string, content: string): void;
  export function error(arg0: string, arg1?: string) {
    log('error', arg0, arg1);
  }
  export function fatal(content: string): void;
  export function fatal(category: string, content: string): void;
  export function fatal(arg0: string, arg1?: string) {
    log('fatal', arg0, arg1);
  }

  export function reset(config?: Logger.Config): void {
    LOGGER_CONFIG_MAP = {};
    set('', config || {});
  }
  export function set(category: string, config: Logger.Config): void {
    if (category === '*') {
      reset(config);
    } else {
      const previousConfig = LOGGER_CONFIG_MAP[category] || LOGGER_DEFAULT_CONFIG;
      LOGGER_CONFIG_MAP[category] = {
        provider: config.provider || previousConfig.provider,
        minimalSeverity: config.minimalSeverity || previousConfig.minimalSeverity,
        logDateTime: config.logDateTime === undefined ? previousConfig.logDateTime : config.logDateTime,
        logSourceLocation:
          config.logSourceLocation === undefined ? previousConfig.logSourceLocation : config.logSourceLocation,
      };
    }

    // TODO: we want to support wildcard or regex?
  }

  export function setWithEnv(env: Env): void {
    const config: Logger.Config = {};
    if (env.logLevel) {
      config.minimalSeverity = env.logLevel as Logger.Severity;
    }
    set('', config);
  }
}

// eslint-disable-next-line @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention
export const Logger: Logger = log;

export declare namespace Profiler {
  export interface Config {
    maxNumberEvents?: number;
    flushBatchSize?: number;
    flushIntervalInMilliseconds?: number;
  }

  export type EventCategory = 'session' | 'node' | 'op' | 'backend';

  export interface Event {
    end(): void | Promise<void>;
  }
}
// TODO
// class WebGLEvent implements Profiler.Event {}

class Event implements Profiler.Event {
  constructor(
    public category: Profiler.EventCategory,
    public name: string,
    public startTime: number,
    private endCallback: (e: Event) => void | Promise<void>,
    public timer?: WebGLQuery,
    public ctx?: WebGLContext,
  ) {}

  async end() {
    return this.endCallback(this);
  }

  async checkTimer(): Promise<number> {
    if (this.ctx === undefined || this.timer === undefined) {
      throw new Error('No webgl timer found');
    } else {
      this.ctx.endTimer();
      return this.ctx.waitForQueryAndGetTime(this.timer);
    }
  }
}

class EventRecord {
  constructor(
    public category: Profiler.EventCategory,
    public name: string,
    public startTime: number,
    public endTime: number,
  ) {}
}

export class Profiler {
  static create(config?: Profiler.Config): Profiler {
    if (config === undefined) {
      return new this();
    }
    return new this(config.maxNumberEvents, config.flushBatchSize, config.flushIntervalInMilliseconds);
  }

  private constructor(maxNumberEvents?: number, flushBatchSize?: number, flushIntervalInMilliseconds?: number) {
    this._started = false;
    this._maxNumberEvents = maxNumberEvents === undefined ? 10000 : maxNumberEvents;
    this._flushBatchSize = flushBatchSize === undefined ? 10 : flushBatchSize;
    this._flushIntervalInMilliseconds = flushIntervalInMilliseconds === undefined ? 5000 : flushIntervalInMilliseconds;
  }

  // start profiling
  start() {
    this._started = true;
    this._timingEvents = [];
    this._flushTime = now();
    this._flushPointer = 0;
  }

  // stop profiling
  stop() {
    this._started = false;
    for (; this._flushPointer < this._timingEvents.length; this._flushPointer++) {
      this.logOneEvent(this._timingEvents[this._flushPointer]);
    }
  }

  // create an event scope for the specific function
  event<T>(category: Profiler.EventCategory, name: string, func: () => T, ctx?: WebGLContext): T;
  event<T>(category: Profiler.EventCategory, name: string, func: () => Promise<T>, ctx?: WebGLContext): Promise<T>;

  event<T>(
    category: Profiler.EventCategory,
    name: string,
    func: () => T | Promise<T>,
    ctx?: WebGLContext,
  ): T | Promise<T> {
    const event = this._started ? this.begin(category, name, ctx) : undefined;
    let isPromise = false;

    const res = func();

    // we consider a then-able object is a promise
    if (res && typeof (res as Promise<T>).then === 'function') {
      isPromise = true;
      return new Promise<T>((resolve, reject) => {
        (res as Promise<T>).then(
          async (value) => {
            // fulfilled
            if (event) {
              await event.end();
            }
            resolve(value);
          },
          async (reason) => {
            // rejected
            if (event) {
              await event.end();
            }
            reject(reason);
          },
        );
      });
    }
    if (!isPromise && event) {
      const eventRes = event.end();
      if (eventRes && typeof eventRes.then === 'function') {
        return new Promise<T>((resolve, reject) => {
          eventRes.then(
            () => {
              // fulfilled
              resolve(res);
            },
            (reason) => {
              // rejected
              reject(reason);
            },
          );
        });
      }
    }
    return res;
  }

  // begin an event
  begin(category: Profiler.EventCategory, name: string, ctx?: WebGLContext): Event {
    if (!this._started) {
      throw new Error('profiler is not started yet');
    }
    if (ctx === undefined) {
      const startTime = now();
      this.flush(startTime);
      return new Event(category, name, startTime, (e) => this.endSync(e));
    } else {
      const timer: WebGLQuery = ctx.beginTimer();
      return new Event(category, name, 0, async (e) => this.end(e), timer, ctx);
    }
  }

  // end the specific event
  private async end(event: Event): Promise<void> {
    const endTime: number = await event.checkTimer();
    if (this._timingEvents.length < this._maxNumberEvents) {
      this._timingEvents.push(new EventRecord(event.category, event.name, event.startTime, endTime));
      this.flush(endTime);
    }
  }

  private endSync(event: Event): void {
    const endTime: number = now();
    if (this._timingEvents.length < this._maxNumberEvents) {
      this._timingEvents.push(new EventRecord(event.category, event.name, event.startTime, endTime));
      this.flush(endTime);
    }
  }

  private logOneEvent(event: EventRecord) {
    Logger.verbose(
      `Profiler.${event.category}`,
      `${(event.endTime - event.startTime).toFixed(2)}ms on event '${event.name}' at ${event.endTime.toFixed(2)}`,
    );
  }

  private flush(currentTime: number) {
    if (
      this._timingEvents.length - this._flushPointer >= this._flushBatchSize ||
      currentTime - this._flushTime >= this._flushIntervalInMilliseconds
    ) {
      // should flush when either batch size accumlated or interval elepsed

      for (
        const previousPointer = this._flushPointer;
        this._flushPointer < previousPointer + this._flushBatchSize && this._flushPointer < this._timingEvents.length;
        this._flushPointer++
      ) {
        this.logOneEvent(this._timingEvents[this._flushPointer]);
      }

      this._flushTime = now();
    }
  }

  get started() {
    return this._started;
  }
  private _started = false;
  private _timingEvents: EventRecord[];

  private readonly _maxNumberEvents: number;

  private readonly _flushBatchSize: number;
  private readonly _flushIntervalInMilliseconds: number;

  private _flushTime: number;
  private _flushPointer = 0;
}

/**
 * returns a number to represent the current timestamp in a resolution as high as possible.
 */
export const now = typeof performance !== 'undefined' && performance.now ? () => performance.now() : Date.now;
