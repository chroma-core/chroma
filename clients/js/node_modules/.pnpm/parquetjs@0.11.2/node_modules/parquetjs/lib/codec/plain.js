'use strict';
const INT53 = require('int53')
const parquet_thrift = require('../../gen-nodejs/parquet_types')

function encodeValues_BOOLEAN(values) {
  let buf = new Buffer(Math.ceil(values.length / 8));
  buf.fill(0);

  for (let i = 0; i < values.length; ++i) {
    if (values[i]) {
      buf[Math.floor(i / 8)] |= (1 << (i % 8));
    }
  }

  return buf;
}

function decodeValues_BOOLEAN(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    let b = cursor.buffer[cursor.offset + Math.floor(i / 8)];
    values.push((b & (1 << (i % 8))) > 0);
  }

  cursor.offset += Math.ceil(count / 8);
  return values;
}

function encodeValues_INT32(values) {
  let buf = new Buffer(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeInt32LE(values[i], i * 4)
  }

  return buf;
}

function decodeValues_INT32(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readInt32LE(cursor.offset));
    cursor.offset += 4;
  }

  return values;
}

function encodeValues_INT64(values) {
  let buf = new Buffer(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    INT53.writeInt64LE(values[i], buf, i * 8);
  }

  return buf;
}

function decodeValues_INT64(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(INT53.readInt64LE(cursor.buffer, cursor.offset));
    cursor.offset += 8;
  }

  return values;
}

function encodeValues_INT96(values) {
  let buf = new Buffer(12 * values.length);

  for (let i = 0; i < values.length; i++) {
    if (values[i] >= 0) {
      INT53.writeInt64LE(values[i], buf, i * 12);
      buf.writeUInt32LE(0, i * 12 + 8); // truncate to 64 actual precision
    } else {
      INT53.writeInt64LE((~-values[i]) + 1, buf, i * 12);
      buf.writeUInt32LE(0xffffffff, i * 12 + 8); // truncate to 64 actual precision
    }
  }

  return buf;
}

function decodeValues_INT96(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    const low = INT53.readInt64LE(cursor.buffer, cursor.offset);
    const high = cursor.buffer.readUInt32LE(cursor.offset + 8);

    if (high === 0xffffffff) {
      values.push((~-low) + 1); // truncate to 64 actual precision
    } else {
      values.push(low); // truncate to 64 actual precision
    }

    cursor.offset += 12;
  }

  return values;
}

function encodeValues_FLOAT(values) {
  let buf = new Buffer(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeFloatLE(values[i], i * 4)
  }

  return buf;
}

function decodeValues_FLOAT(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readFloatLE(cursor.offset));
    cursor.offset += 4;
  }

  return values;
}

function encodeValues_DOUBLE(values) {
  let buf = new Buffer(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeDoubleLE(values[i], i * 8)
  }

  return buf;
}

function decodeValues_DOUBLE(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readDoubleLE(cursor.offset));
    cursor.offset += 8;
  }

  return values;
}

function encodeValues_BYTE_ARRAY(values) {
  let buf_len = 0;
  for (let i = 0; i < values.length; i++) {
    values[i] = Buffer.from(values[i]);
    buf_len += 4 + values[i].length;
  }

  let buf = Buffer.alloc(buf_len);
  let buf_pos = 0;
  for (let i = 0; i < values.length; i++) {
    buf.writeUInt32LE(values[i].length, buf_pos)
    values[i].copy(buf, buf_pos + 4);
    buf_pos += 4 + values[i].length;

  }

  return buf;
}

function decodeValues_BYTE_ARRAY(cursor, count) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    let len = cursor.buffer.readUInt32LE(cursor.offset);
    cursor.offset += 4;
    values.push(cursor.buffer.slice(cursor.offset, cursor.offset + len));
    cursor.offset += len;
  }

  return values;
}



function encodeValues_FIXED_LEN_BYTE_ARRAY(values, opts) {
  if (!opts.typeLength) {
    throw "missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)";
  }

  let buf_len = 0;
  for (let i = 0; i < values.length; i++) {
    values[i] = Buffer.from(values[i]);

    if (values[i].length !== opts.typeLength) {
      throw "invalid value for FIXED_LEN_BYTE_ARRAY: " + values[i];
    }
  }

  return Buffer.concat(values);
}

function decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts) {
  let values = [];

  if (!opts.typeLength) {
    throw "missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)";
  }

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.slice(cursor.offset, cursor.offset + opts.typeLength));
    cursor.offset += opts.typeLength;
  }

  return values;
}

exports.encodeValues = function(type, values, opts) {
  switch (type) {

    case 'BOOLEAN':
      return encodeValues_BOOLEAN(values);

    case 'INT32':
      return encodeValues_INT32(values);

    case 'INT64':
      return encodeValues_INT64(values);

    case 'INT96':
      return encodeValues_INT96(values);

    case 'FLOAT':
      return encodeValues_FLOAT(values);

    case 'DOUBLE':
      return encodeValues_DOUBLE(values);

    case 'BYTE_ARRAY':
      return encodeValues_BYTE_ARRAY(values);

    case 'FIXED_LEN_BYTE_ARRAY':
      return encodeValues_FIXED_LEN_BYTE_ARRAY(values, opts);

    default:
      throw 'unsupported type: ' + type;

  }
}

exports.decodeValues = function(type, cursor, count, opts) {
  switch (type) {

    case 'BOOLEAN':
      return decodeValues_BOOLEAN(cursor, count);

    case 'INT32':
      return decodeValues_INT32(cursor, count);

    case 'INT64':
      return decodeValues_INT64(cursor, count);

    case 'INT96':
      return decodeValues_INT96(cursor, count);

    case 'FLOAT':
      return decodeValues_FLOAT(cursor, count);

    case 'DOUBLE':
      return decodeValues_DOUBLE(cursor, count);

    case 'BYTE_ARRAY':
      return decodeValues_BYTE_ARRAY(cursor, count);

    case 'FIXED_LEN_BYTE_ARRAY':
      return decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts);

    default:
      throw 'unsupported type: ' + type;

  }
}

