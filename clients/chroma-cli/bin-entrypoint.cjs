#!/usr/bin/env node
const { cli } = require("./lib/index.cjs");
cli(...process.argv.slice(2));
