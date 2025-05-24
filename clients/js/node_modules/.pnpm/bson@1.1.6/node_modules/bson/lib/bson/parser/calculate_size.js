'use strict';

var Long = require('../long').Long,
  Double = require('../double').Double,
  Timestamp = require('../timestamp').Timestamp,
  ObjectID = require('../objectid').ObjectID,
  Symbol = require('../symbol').Symbol,
  BSONRegExp = require('../regexp').BSONRegExp,
  Code = require('../code').Code,
  Decimal128 = require('../decimal128'),
  MinKey = require('../min_key').MinKey,
  MaxKey = require('../max_key').MaxKey,
  DBRef = require('../db_ref').DBRef,
  Binary = require('../binary').Binary;

var normalizedFunctionString = require('./utils').normalizedFunctionString;

// To ensure that 0.4 of node works correctly
var isDate = function isDate(d) {
  return typeof d === 'object' && Object.prototype.toString.call(d) === '[object Date]';
};

var calculateObjectSize = function calculateObjectSize(
  object,
  serializeFunctions,
  ignoreUndefined
) {
  var totalLength = 4 + 1;

  if (Array.isArray(object)) {
    for (var i = 0; i < object.length; i++) {
      totalLength += calculateElement(
        i.toString(),
        object[i],
        serializeFunctions,
        true,
        ignoreUndefined
      );
    }
  } else {
    // If we have toBSON defined, override the current object
    if (object.toBSON) {
      object = object.toBSON();
    }

    // Calculate size
    for (var key in object) {
      totalLength += calculateElement(key, object[key], serializeFunctions, false, ignoreUndefined);
    }
  }

  return totalLength;
};

/**
 * @ignore
 * @api private
 */
function calculateElement(name, value, serializeFunctions, isArray, ignoreUndefined) {
  // If we have toBSON defined, override the current object
  if (value && value.toBSON) {
    value = value.toBSON();
  }

  switch (typeof value) {
    case 'string':
      return 1 + Buffer.byteLength(name, 'utf8') + 1 + 4 + Buffer.byteLength(value, 'utf8') + 1;
    case 'number':
      if (Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
        if (value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) {
          // 32 bit
          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (4 + 1);
        } else {
          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
        }
      } else {
        // 64 bit
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
      }
    case 'undefined':
      if (isArray || !ignoreUndefined)
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1;
      return 0;
    case 'boolean':
      return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (1 + 1);
    case 'object':
      if (
        value == null ||
        value instanceof MinKey ||
        value instanceof MaxKey ||
        value['_bsontype'] === 'MinKey' ||
        value['_bsontype'] === 'MaxKey'
      ) {
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1;
      } else if (value instanceof ObjectID || value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (12 + 1);
      } else if (value instanceof Date || isDate(value)) {
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
      } else if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (1 + 4 + 1) + value.length
        );
      } else if (
        value instanceof Long ||
        value instanceof Double ||
        value instanceof Timestamp ||
        value['_bsontype'] === 'Long' ||
        value['_bsontype'] === 'Double' ||
        value['_bsontype'] === 'Timestamp'
      ) {
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
      } else if (value instanceof Decimal128 || value['_bsontype'] === 'Decimal128') {
        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (16 + 1);
      } else if (value instanceof Code || value['_bsontype'] === 'Code') {
        // Calculate size depending on the availability of a scope
        if (value.scope != null && Object.keys(value.scope).length > 0) {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
            1 +
            4 +
            4 +
            Buffer.byteLength(value.code.toString(), 'utf8') +
            1 +
            calculateObjectSize(value.scope, serializeFunctions, ignoreUndefined)
          );
        } else {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
            1 +
            4 +
            Buffer.byteLength(value.code.toString(), 'utf8') +
            1
          );
        }
      } else if (value instanceof Binary || value['_bsontype'] === 'Binary') {
        // Check what kind of subtype we have
        if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
            (value.position + 1 + 4 + 1 + 4)
          );
        } else {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (value.position + 1 + 4 + 1)
          );
        }
      } else if (value instanceof Symbol || value['_bsontype'] === 'Symbol') {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          Buffer.byteLength(value.value, 'utf8') +
          4 +
          1 +
          1
        );
      } else if (value instanceof DBRef || value['_bsontype'] === 'DBRef') {
        // Set up correct object for serialization
        var ordered_values = {
          $ref: value.namespace,
          $id: value.oid
        };

        // Add db reference if it exists
        if (null != value.db) {
          ordered_values['$db'] = value.db;
        }

        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          1 +
          calculateObjectSize(ordered_values, serializeFunctions, ignoreUndefined)
        );
      } else if (
        value instanceof RegExp ||
        Object.prototype.toString.call(value) === '[object RegExp]'
      ) {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          1 +
          Buffer.byteLength(value.source, 'utf8') +
          1 +
          (value.global ? 1 : 0) +
          (value.ignoreCase ? 1 : 0) +
          (value.multiline ? 1 : 0) +
          1
        );
      } else if (value instanceof BSONRegExp || value['_bsontype'] === 'BSONRegExp') {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          1 +
          Buffer.byteLength(value.pattern, 'utf8') +
          1 +
          Buffer.byteLength(value.options, 'utf8') +
          1
        );
      } else {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          calculateObjectSize(value, serializeFunctions, ignoreUndefined) +
          1
        );
      }
    case 'function':
      // WTF for 0.4.X where typeof /someregexp/ === 'function'
      if (
        value instanceof RegExp ||
        Object.prototype.toString.call(value) === '[object RegExp]' ||
        String.call(value) === '[object RegExp]'
      ) {
        return (
          (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
          1 +
          Buffer.byteLength(value.source, 'utf8') +
          1 +
          (value.global ? 1 : 0) +
          (value.ignoreCase ? 1 : 0) +
          (value.multiline ? 1 : 0) +
          1
        );
      } else {
        if (serializeFunctions && value.scope != null && Object.keys(value.scope).length > 0) {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
            1 +
            4 +
            4 +
            Buffer.byteLength(normalizedFunctionString(value), 'utf8') +
            1 +
            calculateObjectSize(value.scope, serializeFunctions, ignoreUndefined)
          );
        } else if (serializeFunctions) {
          return (
            (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) +
            1 +
            4 +
            Buffer.byteLength(normalizedFunctionString(value), 'utf8') +
            1
          );
        }
      }
  }

  return 0;
}

var BSON = {};

// BSON MAX VALUES
BSON.BSON_INT32_MAX = 0x7fffffff;
BSON.BSON_INT32_MIN = -0x80000000;

// JS MAX PRECISE VALUES
BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

module.exports = calculateObjectSize;
