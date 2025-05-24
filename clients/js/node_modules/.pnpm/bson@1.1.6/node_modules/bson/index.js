var BSON = require('./lib/bson/bson'),
  Binary = require('./lib/bson/binary'),
  Code = require('./lib/bson/code'),
  DBRef = require('./lib/bson/db_ref'),
  Decimal128 = require('./lib/bson/decimal128'),
  Double = require('./lib/bson/double'),
  Int32 = require('./lib/bson/int_32'),
  Long = require('./lib/bson/long'),
  Map = require('./lib/bson/map'),
  MaxKey = require('./lib/bson/max_key'),
  MinKey = require('./lib/bson/min_key'),
  ObjectId = require('./lib/bson/objectid'),
  BSONRegExp = require('./lib/bson/regexp'),
  Symbol = require('./lib/bson/symbol'),
  Timestamp = require('./lib/bson/timestamp');

// BSON MAX VALUES
BSON.BSON_INT32_MAX = 0x7fffffff;
BSON.BSON_INT32_MIN = -0x80000000;

BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
BSON.BSON_INT64_MIN = -Math.pow(2, 63);

// JS MAX PRECISE VALUES
BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

// Add BSON types to function creation
BSON.Binary = Binary;
BSON.Code = Code;
BSON.DBRef = DBRef;
BSON.Decimal128 = Decimal128;
BSON.Double = Double;
BSON.Int32 = Int32;
BSON.Long = Long;
BSON.Map = Map;
BSON.MaxKey = MaxKey;
BSON.MinKey = MinKey;
BSON.ObjectId = ObjectId;
BSON.ObjectID = ObjectId;
BSON.BSONRegExp = BSONRegExp;
BSON.Symbol = Symbol;
BSON.Timestamp = Timestamp;

// Return the BSON
module.exports = BSON;
