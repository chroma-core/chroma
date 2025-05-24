"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.encode = exports.toObject = exports.builder = exports.toReference = void 0;
/* eslint-disable @typescript-eslint/no-namespace */
const builder_js_1 = require("./flexbuffers/builder.js");
const reference_js_1 = require("./flexbuffers/reference.js");
var reference_js_2 = require("./flexbuffers/reference.js");
Object.defineProperty(exports, "toReference", { enumerable: true, get: function () { return reference_js_2.toReference; } });
function builder() {
    return new builder_js_1.Builder();
}
exports.builder = builder;
function toObject(buffer) {
    return (0, reference_js_1.toReference)(buffer).toObject();
}
exports.toObject = toObject;
function encode(object, size = 2048, deduplicateStrings = true, deduplicateKeys = true, deduplicateKeyVectors = true) {
    const builder = new builder_js_1.Builder(size > 0 ? size : 2048, deduplicateStrings, deduplicateKeys, deduplicateKeyVectors);
    builder.add(object);
    return builder.finish();
}
exports.encode = encode;
