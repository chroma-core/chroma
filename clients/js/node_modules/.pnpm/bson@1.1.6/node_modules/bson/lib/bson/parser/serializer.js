'use strict';

var writeIEEE754 = require('../float_parser').writeIEEE754,
  Long = require('../long').Long,
  Map = require('../map'),
  Binary = require('../binary').Binary;

var normalizedFunctionString = require('./utils').normalizedFunctionString;

// try {
//   var _Buffer = Uint8Array;
// } catch (e) {
//   _Buffer = Buffer;
// }

var regexp = /\x00/; // eslint-disable-line no-control-regex
var ignoreKeys = ['$db', '$ref', '$id', '$clusterTime'];

// To ensure that 0.4 of node works correctly
var isDate = function isDate(d) {
  return typeof d === 'object' && Object.prototype.toString.call(d) === '[object Date]';
};

var isRegExp = function isRegExp(d) {
  return Object.prototype.toString.call(d) === '[object RegExp]';
};

var serializeString = function(buffer, key, value, index, isArray) {
  // Encode String type
  buffer[index++] = BSON.BSON_DATA_STRING;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes + 1;
  buffer[index - 1] = 0;
  // Write the string
  var size = buffer.write(value, index + 4, 'utf8');
  // Write the size of the string to buffer
  buffer[index + 3] = ((size + 1) >> 24) & 0xff;
  buffer[index + 2] = ((size + 1) >> 16) & 0xff;
  buffer[index + 1] = ((size + 1) >> 8) & 0xff;
  buffer[index] = (size + 1) & 0xff;
  // Update index
  index = index + 4 + size;
  // Write zero
  buffer[index++] = 0;
  return index;
};

var serializeNumber = function(buffer, key, value, index, isArray) {
  // We have an integer value
  if (Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
    // If the value fits in 32 bits encode as int, if it fits in a double
    // encode it as a double, otherwise long
    if (value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) {
      // Set int type 32 bits or less
      buffer[index++] = BSON.BSON_DATA_INT;
      // Number of written bytes
      var numberOfWrittenBytes = !isArray
        ? buffer.write(key, index, 'utf8')
        : buffer.write(key, index, 'ascii');
      // Encode the name
      index = index + numberOfWrittenBytes;
      buffer[index++] = 0;
      // Write the int value
      buffer[index++] = value & 0xff;
      buffer[index++] = (value >> 8) & 0xff;
      buffer[index++] = (value >> 16) & 0xff;
      buffer[index++] = (value >> 24) & 0xff;
    } else if (value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
      // Encode as double
      buffer[index++] = BSON.BSON_DATA_NUMBER;
      // Number of written bytes
      numberOfWrittenBytes = !isArray
        ? buffer.write(key, index, 'utf8')
        : buffer.write(key, index, 'ascii');
      // Encode the name
      index = index + numberOfWrittenBytes;
      buffer[index++] = 0;
      // Write float
      writeIEEE754(buffer, value, index, 'little', 52, 8);
      // Ajust index
      index = index + 8;
    } else {
      // Set long type
      buffer[index++] = BSON.BSON_DATA_LONG;
      // Number of written bytes
      numberOfWrittenBytes = !isArray
        ? buffer.write(key, index, 'utf8')
        : buffer.write(key, index, 'ascii');
      // Encode the name
      index = index + numberOfWrittenBytes;
      buffer[index++] = 0;
      var longVal = Long.fromNumber(value);
      var lowBits = longVal.getLowBits();
      var highBits = longVal.getHighBits();
      // Encode low bits
      buffer[index++] = lowBits & 0xff;
      buffer[index++] = (lowBits >> 8) & 0xff;
      buffer[index++] = (lowBits >> 16) & 0xff;
      buffer[index++] = (lowBits >> 24) & 0xff;
      // Encode high bits
      buffer[index++] = highBits & 0xff;
      buffer[index++] = (highBits >> 8) & 0xff;
      buffer[index++] = (highBits >> 16) & 0xff;
      buffer[index++] = (highBits >> 24) & 0xff;
    }
  } else {
    // Encode as double
    buffer[index++] = BSON.BSON_DATA_NUMBER;
    // Number of written bytes
    numberOfWrittenBytes = !isArray
      ? buffer.write(key, index, 'utf8')
      : buffer.write(key, index, 'ascii');
    // Encode the name
    index = index + numberOfWrittenBytes;
    buffer[index++] = 0;
    // Write float
    writeIEEE754(buffer, value, index, 'little', 52, 8);
    // Ajust index
    index = index + 8;
  }

  return index;
};

var serializeNull = function(buffer, key, value, index, isArray) {
  // Set long type
  buffer[index++] = BSON.BSON_DATA_NULL;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  return index;
};

var serializeBoolean = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_BOOLEAN;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Encode the boolean value
  buffer[index++] = value ? 1 : 0;
  return index;
};

var serializeDate = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_DATE;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;

  // Write the date
  var dateInMilis = Long.fromNumber(value.getTime());
  var lowBits = dateInMilis.getLowBits();
  var highBits = dateInMilis.getHighBits();
  // Encode low bits
  buffer[index++] = lowBits & 0xff;
  buffer[index++] = (lowBits >> 8) & 0xff;
  buffer[index++] = (lowBits >> 16) & 0xff;
  buffer[index++] = (lowBits >> 24) & 0xff;
  // Encode high bits
  buffer[index++] = highBits & 0xff;
  buffer[index++] = (highBits >> 8) & 0xff;
  buffer[index++] = (highBits >> 16) & 0xff;
  buffer[index++] = (highBits >> 24) & 0xff;
  return index;
};

var serializeRegExp = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_REGEXP;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  if (value.source && value.source.match(regexp) != null) {
    throw Error('value ' + value.source + ' must not contain null bytes');
  }
  // Adjust the index
  index = index + buffer.write(value.source, index, 'utf8');
  // Write zero
  buffer[index++] = 0x00;
  // Write the parameters
  if (value.global) buffer[index++] = 0x73; // s
  if (value.ignoreCase) buffer[index++] = 0x69; // i
  if (value.multiline) buffer[index++] = 0x6d; // m
  // Add ending zero
  buffer[index++] = 0x00;
  return index;
};

var serializeBSONRegExp = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_REGEXP;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;

  // Check the pattern for 0 bytes
  if (value.pattern.match(regexp) != null) {
    // The BSON spec doesn't allow keys with null bytes because keys are
    // null-terminated.
    throw Error('pattern ' + value.pattern + ' must not contain null bytes');
  }

  // Adjust the index
  index = index + buffer.write(value.pattern, index, 'utf8');
  // Write zero
  buffer[index++] = 0x00;
  // Write the options
  index =
    index +
    buffer.write(
      value.options
        .split('')
        .sort()
        .join(''),
      index,
      'utf8'
    );
  // Add ending zero
  buffer[index++] = 0x00;
  return index;
};

var serializeMinMax = function(buffer, key, value, index, isArray) {
  // Write the type of either min or max key
  if (value === null) {
    buffer[index++] = BSON.BSON_DATA_NULL;
  } else if (value._bsontype === 'MinKey') {
    buffer[index++] = BSON.BSON_DATA_MIN_KEY;
  } else {
    buffer[index++] = BSON.BSON_DATA_MAX_KEY;
  }

  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  return index;
};

var serializeObjectId = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_OID;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');

  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;

  // Write the objectId into the shared buffer
  if (typeof value.id === 'string') {
    buffer.write(value.id, index, 'binary');
  } else if (value.id && value.id.copy) {
    value.id.copy(buffer, index, 0, 12);
  } else {
    throw new Error('object [' + JSON.stringify(value) + '] is not a valid ObjectId');
  }

  // Ajust index
  return index + 12;
};

var serializeBuffer = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_BINARY;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Get size of the buffer (current write point)
  var size = value.length;
  // Write the size of the string to buffer
  buffer[index++] = size & 0xff;
  buffer[index++] = (size >> 8) & 0xff;
  buffer[index++] = (size >> 16) & 0xff;
  buffer[index++] = (size >> 24) & 0xff;
  // Write the default subtype
  buffer[index++] = BSON.BSON_BINARY_SUBTYPE_DEFAULT;
  // Copy the content form the binary field to the buffer
  value.copy(buffer, index, 0, size);
  // Adjust the index
  index = index + size;
  return index;
};

var serializeObject = function(
  buffer,
  key,
  value,
  index,
  checkKeys,
  depth,
  serializeFunctions,
  ignoreUndefined,
  isArray,
  path
) {
  for (var i = 0; i < path.length; i++) {
    if (path[i] === value) throw new Error('cyclic dependency detected');
  }

  // Push value to stack
  path.push(value);
  // Write the type
  buffer[index++] = Array.isArray(value) ? BSON.BSON_DATA_ARRAY : BSON.BSON_DATA_OBJECT;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  var endIndex = serializeInto(
    buffer,
    value,
    checkKeys,
    index,
    depth + 1,
    serializeFunctions,
    ignoreUndefined,
    path
  );
  // Pop stack
  path.pop();
  // Write size
  return endIndex;
};

var serializeDecimal128 = function(buffer, key, value, index, isArray) {
  buffer[index++] = BSON.BSON_DATA_DECIMAL128;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Write the data from the value
  value.bytes.copy(buffer, index, 0, 16);
  return index + 16;
};

var serializeLong = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = value._bsontype === 'Long' ? BSON.BSON_DATA_LONG : BSON.BSON_DATA_TIMESTAMP;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Write the date
  var lowBits = value.getLowBits();
  var highBits = value.getHighBits();
  // Encode low bits
  buffer[index++] = lowBits & 0xff;
  buffer[index++] = (lowBits >> 8) & 0xff;
  buffer[index++] = (lowBits >> 16) & 0xff;
  buffer[index++] = (lowBits >> 24) & 0xff;
  // Encode high bits
  buffer[index++] = highBits & 0xff;
  buffer[index++] = (highBits >> 8) & 0xff;
  buffer[index++] = (highBits >> 16) & 0xff;
  buffer[index++] = (highBits >> 24) & 0xff;
  return index;
};

var serializeInt32 = function(buffer, key, value, index, isArray) {
  // Set int type 32 bits or less
  buffer[index++] = BSON.BSON_DATA_INT;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Write the int value
  buffer[index++] = value & 0xff;
  buffer[index++] = (value >> 8) & 0xff;
  buffer[index++] = (value >> 16) & 0xff;
  buffer[index++] = (value >> 24) & 0xff;
  return index;
};

var serializeDouble = function(buffer, key, value, index, isArray) {
  // Encode as double
  buffer[index++] = BSON.BSON_DATA_NUMBER;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Write float
  writeIEEE754(buffer, value, index, 'little', 52, 8);
  // Ajust index
  index = index + 8;
  return index;
};

var serializeFunction = function(buffer, key, value, index, checkKeys, depth, isArray) {
  buffer[index++] = BSON.BSON_DATA_CODE;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Function string
  var functionString = normalizedFunctionString(value);

  // Write the string
  var size = buffer.write(functionString, index + 4, 'utf8') + 1;
  // Write the size of the string to buffer
  buffer[index] = size & 0xff;
  buffer[index + 1] = (size >> 8) & 0xff;
  buffer[index + 2] = (size >> 16) & 0xff;
  buffer[index + 3] = (size >> 24) & 0xff;
  // Update index
  index = index + 4 + size - 1;
  // Write zero
  buffer[index++] = 0;
  return index;
};

var serializeCode = function(
  buffer,
  key,
  value,
  index,
  checkKeys,
  depth,
  serializeFunctions,
  ignoreUndefined,
  isArray
) {
  if (value.scope && typeof value.scope === 'object') {
    // Write the type
    buffer[index++] = BSON.BSON_DATA_CODE_W_SCOPE;
    // Number of written bytes
    var numberOfWrittenBytes = !isArray
      ? buffer.write(key, index, 'utf8')
      : buffer.write(key, index, 'ascii');
    // Encode the name
    index = index + numberOfWrittenBytes;
    buffer[index++] = 0;

    // Starting index
    var startIndex = index;

    // Serialize the function
    // Get the function string
    var functionString = typeof value.code === 'string' ? value.code : value.code.toString();
    // Index adjustment
    index = index + 4;
    // Write string into buffer
    var codeSize = buffer.write(functionString, index + 4, 'utf8') + 1;
    // Write the size of the string to buffer
    buffer[index] = codeSize & 0xff;
    buffer[index + 1] = (codeSize >> 8) & 0xff;
    buffer[index + 2] = (codeSize >> 16) & 0xff;
    buffer[index + 3] = (codeSize >> 24) & 0xff;
    // Write end 0
    buffer[index + 4 + codeSize - 1] = 0;
    // Write the
    index = index + codeSize + 4;

    //
    // Serialize the scope value
    var endIndex = serializeInto(
      buffer,
      value.scope,
      checkKeys,
      index,
      depth + 1,
      serializeFunctions,
      ignoreUndefined
    );
    index = endIndex - 1;

    // Writ the total
    var totalSize = endIndex - startIndex;

    // Write the total size of the object
    buffer[startIndex++] = totalSize & 0xff;
    buffer[startIndex++] = (totalSize >> 8) & 0xff;
    buffer[startIndex++] = (totalSize >> 16) & 0xff;
    buffer[startIndex++] = (totalSize >> 24) & 0xff;
    // Write trailing zero
    buffer[index++] = 0;
  } else {
    buffer[index++] = BSON.BSON_DATA_CODE;
    // Number of written bytes
    numberOfWrittenBytes = !isArray
      ? buffer.write(key, index, 'utf8')
      : buffer.write(key, index, 'ascii');
    // Encode the name
    index = index + numberOfWrittenBytes;
    buffer[index++] = 0;
    // Function string
    functionString = value.code.toString();
    // Write the string
    var size = buffer.write(functionString, index + 4, 'utf8') + 1;
    // Write the size of the string to buffer
    buffer[index] = size & 0xff;
    buffer[index + 1] = (size >> 8) & 0xff;
    buffer[index + 2] = (size >> 16) & 0xff;
    buffer[index + 3] = (size >> 24) & 0xff;
    // Update index
    index = index + 4 + size - 1;
    // Write zero
    buffer[index++] = 0;
  }

  return index;
};

var serializeBinary = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_BINARY;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Extract the buffer
  var data = value.value(true);
  // Calculate size
  var size = value.position;
  // Add the deprecated 02 type 4 bytes of size to total
  if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) size = size + 4;
  // Write the size of the string to buffer
  buffer[index++] = size & 0xff;
  buffer[index++] = (size >> 8) & 0xff;
  buffer[index++] = (size >> 16) & 0xff;
  buffer[index++] = (size >> 24) & 0xff;
  // Write the subtype to the buffer
  buffer[index++] = value.sub_type;

  // If we have binary type 2 the 4 first bytes are the size
  if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) {
    size = size - 4;
    buffer[index++] = size & 0xff;
    buffer[index++] = (size >> 8) & 0xff;
    buffer[index++] = (size >> 16) & 0xff;
    buffer[index++] = (size >> 24) & 0xff;
  }

  // Write the data to the object
  data.copy(buffer, index, 0, value.position);
  // Adjust the index
  index = index + value.position;
  return index;
};

var serializeSymbol = function(buffer, key, value, index, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_SYMBOL;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');
  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;
  // Write the string
  var size = buffer.write(value.value, index + 4, 'utf8') + 1;
  // Write the size of the string to buffer
  buffer[index] = size & 0xff;
  buffer[index + 1] = (size >> 8) & 0xff;
  buffer[index + 2] = (size >> 16) & 0xff;
  buffer[index + 3] = (size >> 24) & 0xff;
  // Update index
  index = index + 4 + size - 1;
  // Write zero
  buffer[index++] = 0x00;
  return index;
};

var serializeDBRef = function(buffer, key, value, index, depth, serializeFunctions, isArray) {
  // Write the type
  buffer[index++] = BSON.BSON_DATA_OBJECT;
  // Number of written bytes
  var numberOfWrittenBytes = !isArray
    ? buffer.write(key, index, 'utf8')
    : buffer.write(key, index, 'ascii');

  // Encode the name
  index = index + numberOfWrittenBytes;
  buffer[index++] = 0;

  var startIndex = index;
  var endIndex;

  // Serialize object
  if (null != value.db) {
    endIndex = serializeInto(
      buffer,
      {
        $ref: value.namespace,
        $id: value.oid,
        $db: value.db
      },
      false,
      index,
      depth + 1,
      serializeFunctions
    );
  } else {
    endIndex = serializeInto(
      buffer,
      {
        $ref: value.namespace,
        $id: value.oid
      },
      false,
      index,
      depth + 1,
      serializeFunctions
    );
  }

  // Calculate object size
  var size = endIndex - startIndex;
  // Write the size
  buffer[startIndex++] = size & 0xff;
  buffer[startIndex++] = (size >> 8) & 0xff;
  buffer[startIndex++] = (size >> 16) & 0xff;
  buffer[startIndex++] = (size >> 24) & 0xff;
  // Set index
  return endIndex;
};

var serializeInto = function serializeInto(
  buffer,
  object,
  checkKeys,
  startingIndex,
  depth,
  serializeFunctions,
  ignoreUndefined,
  path
) {
  startingIndex = startingIndex || 0;
  path = path || [];

  // Push the object to the path
  path.push(object);

  // Start place to serialize into
  var index = startingIndex + 4;
  // var self = this;

  // Special case isArray
  if (Array.isArray(object)) {
    // Get object keys
    for (var i = 0; i < object.length; i++) {
      var key = '' + i;
      var value = object[i];

      // Is there an override value
      if (value && value.toBSON) {
        if (typeof value.toBSON !== 'function') throw new Error('toBSON is not a function');
        value = value.toBSON();
      }

      var type = typeof value;
      if (type === 'string') {
        index = serializeString(buffer, key, value, index, true);
      } else if (type === 'number') {
        index = serializeNumber(buffer, key, value, index, true);
      } else  if(type === 'bigint') {
        throw new TypeError('Unsupported type BigInt, please use Decimal128');
      } else if (type === 'boolean') {
        index = serializeBoolean(buffer, key, value, index, true);
      } else if (value instanceof Date || isDate(value)) {
        index = serializeDate(buffer, key, value, index, true);
      } else if (value === undefined) {
        index = serializeNull(buffer, key, value, index, true);
      } else if (value === null) {
        index = serializeNull(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
        index = serializeObjectId(buffer, key, value, index, true);
      } else if (Buffer.isBuffer(value)) {
        index = serializeBuffer(buffer, key, value, index, true);
      } else if (value instanceof RegExp || isRegExp(value)) {
        index = serializeRegExp(buffer, key, value, index, true);
      } else if (type === 'object' && value['_bsontype'] == null) {
        index = serializeObject(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined,
          true,
          path
        );
      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
        index = serializeDecimal128(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
        index = serializeLong(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'Double') {
        index = serializeDouble(buffer, key, value, index, true);
      } else if (typeof value === 'function' && serializeFunctions) {
        index = serializeFunction(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          true
        );
      } else if (value['_bsontype'] === 'Code') {
        index = serializeCode(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined,
          true
        );
      } else if (value['_bsontype'] === 'Binary') {
        index = serializeBinary(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'Symbol') {
        index = serializeSymbol(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'DBRef') {
        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions, true);
      } else if (value['_bsontype'] === 'BSONRegExp') {
        index = serializeBSONRegExp(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'Int32') {
        index = serializeInt32(buffer, key, value, index, true);
      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
        index = serializeMinMax(buffer, key, value, index, true);
      } else if (typeof value['_bsontype'] !== 'undefined') {
        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
      }
    }
  } else if (object instanceof Map) {
    var iterator = object.entries();
    var done = false;

    while (!done) {
      // Unpack the next entry
      var entry = iterator.next();
      done = entry.done;
      // Are we done, then skip and terminate
      if (done) continue;

      // Get the entry values
      key = entry.value[0];
      value = entry.value[1];

      // Check the type of the value
      type = typeof value;

      // Check the key and throw error if it's illegal
      if (typeof key === 'string' && ignoreKeys.indexOf(key) === -1) {
        if (key.match(regexp) != null) {
          // The BSON spec doesn't allow keys with null bytes because keys are
          // null-terminated.
          throw Error('key ' + key + ' must not contain null bytes');
        }

        if (checkKeys) {
          if ('$' === key[0]) {
            throw Error('key ' + key + " must not start with '$'");
          } else if (~key.indexOf('.')) {
            throw Error('key ' + key + " must not contain '.'");
          }
        }
      }

      if (type === 'string') {
        index = serializeString(buffer, key, value, index);
      } else if (type === 'number') {
        index = serializeNumber(buffer, key, value, index);
      } else if(type === 'bigint') {
        throw new TypeError('Unsupported type BigInt, please use Decimal128');
      } else if (type === 'boolean') {
        index = serializeBoolean(buffer, key, value, index);
      } else if (value instanceof Date || isDate(value)) {
        index = serializeDate(buffer, key, value, index);
        // } else if (value === undefined && ignoreUndefined === true) {
      } else if (value === null || (value === undefined && ignoreUndefined === false)) {
        index = serializeNull(buffer, key, value, index);
      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
        index = serializeObjectId(buffer, key, value, index);
      } else if (Buffer.isBuffer(value)) {
        index = serializeBuffer(buffer, key, value, index);
      } else if (value instanceof RegExp || isRegExp(value)) {
        index = serializeRegExp(buffer, key, value, index);
      } else if (type === 'object' && value['_bsontype'] == null) {
        index = serializeObject(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined,
          false,
          path
        );
      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
        index = serializeDecimal128(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
        index = serializeLong(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Double') {
        index = serializeDouble(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Code') {
        index = serializeCode(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined
        );
      } else if (typeof value === 'function' && serializeFunctions) {
        index = serializeFunction(buffer, key, value, index, checkKeys, depth, serializeFunctions);
      } else if (value['_bsontype'] === 'Binary') {
        index = serializeBinary(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Symbol') {
        index = serializeSymbol(buffer, key, value, index);
      } else if (value['_bsontype'] === 'DBRef') {
        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions);
      } else if (value['_bsontype'] === 'BSONRegExp') {
        index = serializeBSONRegExp(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Int32') {
        index = serializeInt32(buffer, key, value, index);
      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
        index = serializeMinMax(buffer, key, value, index);
      } else if (typeof value['_bsontype'] !== 'undefined') {
        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
      }
    }
  } else {
    // Did we provide a custom serialization method
    if (object.toBSON) {
      if (typeof object.toBSON !== 'function') throw new Error('toBSON is not a function');
      object = object.toBSON();
      if (object != null && typeof object !== 'object')
        throw new Error('toBSON function did not return an object');
    }

    // Iterate over all the keys
    for (key in object) {
      value = object[key];
      // Is there an override value
      if (value && value.toBSON) {
        if (typeof value.toBSON !== 'function') throw new Error('toBSON is not a function');
        value = value.toBSON();
      }

      // Check the type of the value
      type = typeof value;

      // Check the key and throw error if it's illegal
      if (typeof key === 'string' && ignoreKeys.indexOf(key) === -1) {
        if (key.match(regexp) != null) {
          // The BSON spec doesn't allow keys with null bytes because keys are
          // null-terminated.
          throw Error('key ' + key + ' must not contain null bytes');
        }

        if (checkKeys) {
          if ('$' === key[0]) {
            throw Error('key ' + key + " must not start with '$'");
          } else if (~key.indexOf('.')) {
            throw Error('key ' + key + " must not contain '.'");
          }
        }
      }

      if (type === 'string') {
        index = serializeString(buffer, key, value, index);
      } else if (type === 'number') {
        index = serializeNumber(buffer, key, value, index);
      } else if(type === 'bigint') {
        throw new TypeError('Unsupported type BigInt, please use Decimal128');
      } else if (type === 'boolean') {
        index = serializeBoolean(buffer, key, value, index);
      } else if (value instanceof Date || isDate(value)) {
        index = serializeDate(buffer, key, value, index);
      } else if (value === undefined) {
        if (ignoreUndefined === false) index = serializeNull(buffer, key, value, index);
      } else if (value === null) {
        index = serializeNull(buffer, key, value, index);
      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
        index = serializeObjectId(buffer, key, value, index);
      } else if (Buffer.isBuffer(value)) {
        index = serializeBuffer(buffer, key, value, index);
      } else if (value instanceof RegExp || isRegExp(value)) {
        index = serializeRegExp(buffer, key, value, index);
      } else if (type === 'object' && value['_bsontype'] == null) {
        index = serializeObject(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined,
          false,
          path
        );
      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
        index = serializeDecimal128(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
        index = serializeLong(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Double') {
        index = serializeDouble(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Code') {
        index = serializeCode(
          buffer,
          key,
          value,
          index,
          checkKeys,
          depth,
          serializeFunctions,
          ignoreUndefined
        );
      } else if (typeof value === 'function' && serializeFunctions) {
        index = serializeFunction(buffer, key, value, index, checkKeys, depth, serializeFunctions);
      } else if (value['_bsontype'] === 'Binary') {
        index = serializeBinary(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Symbol') {
        index = serializeSymbol(buffer, key, value, index);
      } else if (value['_bsontype'] === 'DBRef') {
        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions);
      } else if (value['_bsontype'] === 'BSONRegExp') {
        index = serializeBSONRegExp(buffer, key, value, index);
      } else if (value['_bsontype'] === 'Int32') {
        index = serializeInt32(buffer, key, value, index);
      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
        index = serializeMinMax(buffer, key, value, index);
      } else if (typeof value['_bsontype'] !== 'undefined') {
        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
      }
    }
  }

  // Remove the path
  path.pop();

  // Final padding byte for object
  buffer[index++] = 0x00;

  // Final size
  var size = index - startingIndex;
  // Write the size of the object
  buffer[startingIndex++] = size & 0xff;
  buffer[startingIndex++] = (size >> 8) & 0xff;
  buffer[startingIndex++] = (size >> 16) & 0xff;
  buffer[startingIndex++] = (size >> 24) & 0xff;
  return index;
};

var BSON = {};

/**
 * Contains the function cache if we have that enable to allow for avoiding the eval step on each deserialization, comparison is by md5
 *
 * @ignore
 * @api private
 */
// var functionCache = (BSON.functionCache = {});

/**
 * Number BSON Type
 *
 * @classconstant BSON_DATA_NUMBER
 **/
BSON.BSON_DATA_NUMBER = 1;
/**
 * String BSON Type
 *
 * @classconstant BSON_DATA_STRING
 **/
BSON.BSON_DATA_STRING = 2;
/**
 * Object BSON Type
 *
 * @classconstant BSON_DATA_OBJECT
 **/
BSON.BSON_DATA_OBJECT = 3;
/**
 * Array BSON Type
 *
 * @classconstant BSON_DATA_ARRAY
 **/
BSON.BSON_DATA_ARRAY = 4;
/**
 * Binary BSON Type
 *
 * @classconstant BSON_DATA_BINARY
 **/
BSON.BSON_DATA_BINARY = 5;
/**
 * ObjectID BSON Type, deprecated
 *
 * @classconstant BSON_DATA_UNDEFINED
 **/
BSON.BSON_DATA_UNDEFINED = 6;
/**
 * ObjectID BSON Type
 *
 * @classconstant BSON_DATA_OID
 **/
BSON.BSON_DATA_OID = 7;
/**
 * Boolean BSON Type
 *
 * @classconstant BSON_DATA_BOOLEAN
 **/
BSON.BSON_DATA_BOOLEAN = 8;
/**
 * Date BSON Type
 *
 * @classconstant BSON_DATA_DATE
 **/
BSON.BSON_DATA_DATE = 9;
/**
 * null BSON Type
 *
 * @classconstant BSON_DATA_NULL
 **/
BSON.BSON_DATA_NULL = 10;
/**
 * RegExp BSON Type
 *
 * @classconstant BSON_DATA_REGEXP
 **/
BSON.BSON_DATA_REGEXP = 11;
/**
 * Code BSON Type
 *
 * @classconstant BSON_DATA_CODE
 **/
BSON.BSON_DATA_CODE = 13;
/**
 * Symbol BSON Type
 *
 * @classconstant BSON_DATA_SYMBOL
 **/
BSON.BSON_DATA_SYMBOL = 14;
/**
 * Code with Scope BSON Type
 *
 * @classconstant BSON_DATA_CODE_W_SCOPE
 **/
BSON.BSON_DATA_CODE_W_SCOPE = 15;
/**
 * 32 bit Integer BSON Type
 *
 * @classconstant BSON_DATA_INT
 **/
BSON.BSON_DATA_INT = 16;
/**
 * Timestamp BSON Type
 *
 * @classconstant BSON_DATA_TIMESTAMP
 **/
BSON.BSON_DATA_TIMESTAMP = 17;
/**
 * Long BSON Type
 *
 * @classconstant BSON_DATA_LONG
 **/
BSON.BSON_DATA_LONG = 18;
/**
 * Long BSON Type
 *
 * @classconstant BSON_DATA_DECIMAL128
 **/
BSON.BSON_DATA_DECIMAL128 = 19;
/**
 * MinKey BSON Type
 *
 * @classconstant BSON_DATA_MIN_KEY
 **/
BSON.BSON_DATA_MIN_KEY = 0xff;
/**
 * MaxKey BSON Type
 *
 * @classconstant BSON_DATA_MAX_KEY
 **/
BSON.BSON_DATA_MAX_KEY = 0x7f;
/**
 * Binary Default Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_DEFAULT
 **/
BSON.BSON_BINARY_SUBTYPE_DEFAULT = 0;
/**
 * Binary Function Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_FUNCTION
 **/
BSON.BSON_BINARY_SUBTYPE_FUNCTION = 1;
/**
 * Binary Byte Array Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_BYTE_ARRAY
 **/
BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
/**
 * Binary UUID Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_UUID
 **/
BSON.BSON_BINARY_SUBTYPE_UUID = 3;
/**
 * Binary MD5 Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_MD5
 **/
BSON.BSON_BINARY_SUBTYPE_MD5 = 4;
/**
 * Binary User Defined Type
 *
 * @classconstant BSON_BINARY_SUBTYPE_USER_DEFINED
 **/
BSON.BSON_BINARY_SUBTYPE_USER_DEFINED = 128;

// BSON MAX VALUES
BSON.BSON_INT32_MAX = 0x7fffffff;
BSON.BSON_INT32_MIN = -0x80000000;

BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
BSON.BSON_INT64_MIN = -Math.pow(2, 63);

// JS MAX PRECISE VALUES
BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

// Internal long versions
// var JS_INT_MAX_LONG = Long.fromNumber(0x20000000000000); // Any integer up to 2^53 can be precisely represented by a double.
// var JS_INT_MIN_LONG = Long.fromNumber(-0x20000000000000); // Any integer down to -2^53 can be precisely represented by a double.

module.exports = serializeInto;
