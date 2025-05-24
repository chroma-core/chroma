'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.now = exports.Profiler = exports.Logger = void 0;
class NoOpLoggerProvider {
  log(_severity, _content, _category) {
    // do nothing
  }
}
class ConsoleLoggerProvider {
  log(severity, content, category) {
    // eslint-disable-next-line no-console
    console.log(`${this.color(severity)} ${category ? '\x1b[35m' + category + '\x1b[0m ' : ''}${content}`);
  }
  color(severity) {
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
const LOGGER_PROVIDER_MAP = {
  ['none']: new NoOpLoggerProvider(),
  ['console']: new ConsoleLoggerProvider(),
};
const LOGGER_DEFAULT_CONFIG = {
  provider: 'console',
  minimalSeverity: 'warning',
  logDateTime: true,
  logSourceLocation: false,
};
let LOGGER_CONFIG_MAP = {
  ['']: LOGGER_DEFAULT_CONFIG,
};
function log(arg0, arg1, arg2, arg3) {
  if (arg1 === undefined) {
    // log(category: string): Logger.CategorizedLogger;
    return createCategorizedLogger(arg0);
  } else if (arg2 === undefined) {
    // log(severity, content);
    logInternal(arg0, arg1, 1);
  } else if (typeof arg2 === 'number' && arg3 === undefined) {
    // log(severity, content, stack)
    logInternal(arg0, arg1, arg2);
  } else if (typeof arg2 === 'string' && arg3 === undefined) {
    // log(severity, category, content)
    logInternal(arg0, arg2, 1, arg1);
  } else if (typeof arg2 === 'string' && typeof arg3 === 'number') {
    // log(severity, category, content, stack)
    logInternal(arg0, arg2, arg3, arg1);
  } else {
    throw new TypeError('input is valid');
  }
}
function createCategorizedLogger(category) {
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
function logInternal(severity, content, _stack, category) {
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
(function (log) {
  function verbose(arg0, arg1) {
    log('verbose', arg0, arg1);
  }
  log.verbose = verbose;
  function info(arg0, arg1) {
    log('info', arg0, arg1);
  }
  log.info = info;
  function warning(arg0, arg1) {
    log('warning', arg0, arg1);
  }
  log.warning = warning;
  function error(arg0, arg1) {
    log('error', arg0, arg1);
  }
  log.error = error;
  function fatal(arg0, arg1) {
    log('fatal', arg0, arg1);
  }
  log.fatal = fatal;
  function reset(config) {
    LOGGER_CONFIG_MAP = {};
    set('', config || {});
  }
  log.reset = reset;
  function set(category, config) {
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
  log.set = set;
  function setWithEnv(env) {
    const config = {};
    if (env.logLevel) {
      config.minimalSeverity = env.logLevel;
    }
    set('', config);
  }
  log.setWithEnv = setWithEnv;
})(log || (log = {}));
// eslint-disable-next-line @typescript-eslint/no-redeclare, @typescript-eslint/naming-convention
exports.Logger = log;
// TODO
// class WebGLEvent implements Profiler.Event {}
class Event {
  constructor(category, name, startTime, endCallback, timer, ctx) {
    this.category = category;
    this.name = name;
    this.startTime = startTime;
    this.endCallback = endCallback;
    this.timer = timer;
    this.ctx = ctx;
  }
  async end() {
    return this.endCallback(this);
  }
  async checkTimer() {
    if (this.ctx === undefined || this.timer === undefined) {
      throw new Error('No webgl timer found');
    } else {
      this.ctx.endTimer();
      return this.ctx.waitForQueryAndGetTime(this.timer);
    }
  }
}
class EventRecord {
  constructor(category, name, startTime, endTime) {
    this.category = category;
    this.name = name;
    this.startTime = startTime;
    this.endTime = endTime;
  }
}
class Profiler {
  static create(config) {
    if (config === undefined) {
      return new this();
    }
    return new this(config.maxNumberEvents, config.flushBatchSize, config.flushIntervalInMilliseconds);
  }
  constructor(maxNumberEvents, flushBatchSize, flushIntervalInMilliseconds) {
    this._started = false;
    this._flushPointer = 0;
    this._started = false;
    this._maxNumberEvents = maxNumberEvents === undefined ? 10000 : maxNumberEvents;
    this._flushBatchSize = flushBatchSize === undefined ? 10 : flushBatchSize;
    this._flushIntervalInMilliseconds = flushIntervalInMilliseconds === undefined ? 5000 : flushIntervalInMilliseconds;
  }
  // start profiling
  start() {
    this._started = true;
    this._timingEvents = [];
    this._flushTime = (0, exports.now)();
    this._flushPointer = 0;
  }
  // stop profiling
  stop() {
    this._started = false;
    for (; this._flushPointer < this._timingEvents.length; this._flushPointer++) {
      this.logOneEvent(this._timingEvents[this._flushPointer]);
    }
  }
  event(category, name, func, ctx) {
    const event = this._started ? this.begin(category, name, ctx) : undefined;
    let isPromise = false;
    const res = func();
    // we consider a then-able object is a promise
    if (res && typeof res.then === 'function') {
      isPromise = true;
      return new Promise((resolve, reject) => {
        res.then(
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
        return new Promise((resolve, reject) => {
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
  begin(category, name, ctx) {
    if (!this._started) {
      throw new Error('profiler is not started yet');
    }
    if (ctx === undefined) {
      const startTime = (0, exports.now)();
      this.flush(startTime);
      return new Event(category, name, startTime, (e) => this.endSync(e));
    } else {
      const timer = ctx.beginTimer();
      return new Event(category, name, 0, async (e) => this.end(e), timer, ctx);
    }
  }
  // end the specific event
  async end(event) {
    const endTime = await event.checkTimer();
    if (this._timingEvents.length < this._maxNumberEvents) {
      this._timingEvents.push(new EventRecord(event.category, event.name, event.startTime, endTime));
      this.flush(endTime);
    }
  }
  endSync(event) {
    const endTime = (0, exports.now)();
    if (this._timingEvents.length < this._maxNumberEvents) {
      this._timingEvents.push(new EventRecord(event.category, event.name, event.startTime, endTime));
      this.flush(endTime);
    }
  }
  logOneEvent(event) {
    exports.Logger.verbose(
      `Profiler.${event.category}`,
      `${(event.endTime - event.startTime).toFixed(2)}ms on event '${event.name}' at ${event.endTime.toFixed(2)}`,
    );
  }
  flush(currentTime) {
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
      this._flushTime = (0, exports.now)();
    }
  }
  get started() {
    return this._started;
  }
}
exports.Profiler = Profiler;
/**
 * returns a number to represent the current timestamp in a resolution as high as possible.
 */
exports.now = typeof performance !== 'undefined' && performance.now ? () => performance.now() : Date.now;
//# sourceMappingURL=instrument.js.map
