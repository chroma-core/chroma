const { createRequire } = require("node:module");
const _createJiti = require("../dist/jiti.cjs");
const transform = require("../dist/babel.cjs");

function onError(err) {
  throw err; /* ↓ Check stack trace ↓ */
}

const nativeImport = (id) => import(id);

function createJiti(id, opts = {}) {
  if (!opts.transform) {
    opts = { ...opts, transform };
  }
  return _createJiti(id, opts, {
    onError,
    nativeImport,
    createRequire,
  });
}

module.exports = createJiti;
module.exports.createJiti = createJiti;
