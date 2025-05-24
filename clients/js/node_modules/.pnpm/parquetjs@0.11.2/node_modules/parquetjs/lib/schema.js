'use strict';
const parquet_codec = require('./codec');
const parquet_compression = require('./compression');
const parquet_types = require('./types');
const parquet_util = require('./util');

const PARQUET_COLUMN_KEY_SEPARATOR = '.';

/**
 * A parquet file schema
 */
class ParquetSchema {

  /**
   * Create a new schema from a JSON schema definition
   */
  constructor(schema) {
    this.schema = schema;
    this.fields = buildFields(schema);
    this.fieldList = listFields(this.fields);
  }

  /**
   * Retrieve a field definition
   */
  findField(path) {
    if (path.constructor !== Array) {
      path = path.split(",");
    } else {
      path = path.slice(0); // clone array
    }

    let n = this.fields;
    for (; path.length > 1; path.shift()) {
      n = n[path[0]].fields;
    }

    return n[path[0]];
  }

  /**
   * Retrieve a field definition and all the field's ancestors
   */
  findFieldBranch(path) {
    if (path.constructor !== Array) {
      path = path.split(",");
    }

    let branch = [];
    let n = this.fields;
    for (; path.length > 0; path.shift()) {
      branch.push(n[path[0]]);

      if (path.length > 1) {
        n = n[path[0]].fields;
      }
    }

    return branch;
  }

};

function buildFields(schema, rLevelParentMax, dLevelParentMax, path) {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  if (!path) {
    path = [];
  }

  let fieldList = {};
  for (let name in schema) {
    const opts = schema[name];

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetitionType = 'REQUIRED';
    if (!required) {
      repetitionType = 'OPTIONAL';
      ++dLevelMax;
    }

    if (repeated) {
      repetitionType = 'REPEATED';
      ++rLevelMax;

      if (required) {
        ++dLevelMax;
      }
    }

    /* nested field */
    if (opts.fields) {
      fieldList[name] = {
        name: name,
        path: path.concat([name]),
        repetitionType: repetitionType,
        rLevelMax: rLevelMax,
        dLevelMax: dLevelMax,
        isNested: true,
        fieldCount: Object.keys(opts.fields).length,
        fields: buildFields(
              opts.fields,
              rLevelMax,
              dLevelMax,
              path.concat([name]))
      };

      continue;
    }

    /* field type */
    const typeDef = parquet_types.PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw 'invalid parquet type: ' + opts.type;
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (!(opts.encoding in parquet_codec)) {
      throw 'unsupported parquet encoding: ' + opts.encodig;
    }

    if (!opts.compression) {
      opts.compression = 'UNCOMPRESSED';
    }

    if (!(opts.compression in parquet_compression.PARQUET_COMPRESSION_METHODS)) {
      throw 'unsupported compression method: ' + opts.compression;
    }

    /* add to schema */
    fieldList[name] = {
      name: name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: path.concat([name]),
      repetitionType: repetitionType,
      encoding: opts.encoding,
      compression: opts.compression,
      typeLength: opts.typeLength || typeDef.typeLength,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax
    };
  }

  return fieldList;
}

function listFields(fields) {
  let list = [];

  for (let k in fields) {
    list.push(fields[k]);

    if (fields[k].isNested) {
      list = list.concat(listFields(fields[k].fields));
    }
  }

  return list;
}

module.exports = { ParquetSchema };

