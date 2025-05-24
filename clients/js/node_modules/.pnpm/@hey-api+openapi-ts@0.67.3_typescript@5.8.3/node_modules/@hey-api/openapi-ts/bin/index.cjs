#!/usr/bin/env node

'use strict';

const path = require('path');

const { program } = require('commander');
const pkg = require('../package.json');

const params = program
  .name(Object.keys(pkg.bin)[0])
  .usage('[options]')
  .version(pkg.version)
  .option(
    '-c, --client <value>',
    'HTTP client to generate [@hey-api/client-axios, @hey-api/client-fetch, @hey-api/client-next, @hey-api/client-nuxt, legacy/angular, legacy/axios, legacy/fetch, legacy/node, legacy/xhr]',
  )
  .option('-d, --debug', 'Set log level to debug')
  .option('--dry-run [value]', 'Skip writing files to disk?')
  .option(
    '-e, --experimental-parser [value]',
    'Opt-in to the experimental parser?',
  )
  .option('-f, --file [value]', 'Path to the config file')
  .option(
    '-i, --input <value>',
    'OpenAPI specification (path, url, or string content)',
  )
  .option('-l, --logs [value]', 'Logs folder')
  .option('-o, --output <value>', 'Output folder')
  .option('-p, --plugins [value...]', "List of plugins you'd like to use")
  .option(
    '--base [value]',
    'DEPRECATED. Manually set base in OpenAPI config instead of inferring from server value',
  )
  .option('-s, --silent', 'Set log level to silent')
  .option(
    '--no-log-file',
    'Disable writing a log file. Works like --silent but without supressing console output',
  )
  .option(
    '-w, --watch [value]',
    'Regenerate the client when the input file changes?',
  )
  .option('--exportCore [value]', 'DEPRECATED. Write core files to disk')
  .option('--name <value>', 'DEPRECATED. Custom client class name')
  .option('--request <value>', 'DEPRECATED. Path to custom request file')
  .option(
    '--useOptions [value]',
    'DEPRECATED. Use options instead of arguments?',
  )
  .parse(process.argv)
  .opts();

const stringToBoolean = (value) => {
  if (value === 'true') {
    return true;
  }
  if (value === 'false') {
    return false;
  }
  return value;
};

const processParams = (obj, booleanKeys) => {
  for (const key of booleanKeys) {
    const value = obj[key];
    if (typeof value === 'string') {
      const parsedValue = stringToBoolean(value);
      delete obj[key];
      obj[key] = parsedValue;
    }
  }
  if (obj.file) {
    obj.configFile = obj.file;
  }
  return obj;
};

async function start() {
  let userConfig;

  try {
    const { createClient } = require(
      path.resolve(__dirname, '../dist/index.cjs'),
    );

    userConfig = processParams(params, [
      'dryRun',
      'logFile',
      'experimentalParser',
      'exportCore',
      'useOptions',
    ]);

    if (params.plugins === true) {
      userConfig.plugins = [];
    } else if (params.plugins) {
      userConfig.plugins = params.plugins;
    } else if (userConfig.client) {
      userConfig.plugins = ['@hey-api/typescript', '@hey-api/sdk'];
    }

    if (userConfig.client) {
      userConfig.plugins.push(userConfig.client);
      delete userConfig.client;
    }

    userConfig.logs = userConfig.logs
      ? {
          path: userConfig.logs,
        }
      : {};

    if (userConfig.debug || stringToBoolean(process.env.DEBUG)) {
      userConfig.logs.level = 'debug';
    } else if (userConfig.silent) {
      userConfig.logs.level = 'silent';
    }

    userConfig.logs.file = userConfig.logFile;

    if (typeof params.watch === 'string') {
      userConfig.watch = Number.parseInt(params.watch, 10);
    }

    if (!Object.keys(userConfig.logs).length) {
      delete userConfig.logs;
    }

    const context = await createClient(userConfig);
    if (!context[0] || !context[0].config.watch) {
      process.exit(0);
    }
  } catch {
    process.exit(1);
  }
}

start();
