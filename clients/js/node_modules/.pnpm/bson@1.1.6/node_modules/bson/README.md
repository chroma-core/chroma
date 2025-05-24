# BSON parser

BSON is short for Bin­ary JSON and is the bin­ary-en­coded seri­al­iz­a­tion of JSON-like doc­u­ments. You can learn more about it in [the specification](http://bsonspec.org).

This browser version of the BSON parser is compiled using [webpack](https://webpack.js.org/) and the current version is pre-compiled in the `browser_build` directory.

This is the default BSON parser, however, there is a C++ Node.js addon version as well that does not support the browser. It can be found at [mongod-js/bson-ext](https://github.com/mongodb-js/bson-ext).

## Usage

To build a new version perform the following operations:

```
npm install
npm run build
```

A simple example of how to use BSON in the browser:

```html
<script src="./browser_build/bson.js"></script>

<script>
  function start() {
    // Get the Long type
    var Long = BSON.Long;
    // Create a bson parser instance
    var bson = new BSON();

    // Serialize document
    var doc = { long: Long.fromNumber(100) }

    // Serialize a document
    var data = bson.serialize(doc)
    // De serialize it again
    var doc_2 = bson.deserialize(data)
  }
</script>
```

A simple example of how to use BSON in `Node.js`:

```js
// Get BSON parser class
var BSON = require('bson')
// Get the Long type
var Long = BSON.Long;
// Create a bson parser instance
var bson = new BSON();

// Serialize document
var doc = { long: Long.fromNumber(100) }

// Serialize a document
var data = bson.serialize(doc)
console.log('data:', data)

// Deserialize the resulting Buffer
var doc_2 = bson.deserialize(data)
console.log('doc_2:', doc_2)
```

## Installation

`npm install bson`

## API

### BSON types

For all BSON types documentation, please refer to the following sources:
  * [MongoDB BSON Type Reference](https://docs.mongodb.com/manual/reference/bson-types/)
  * [BSON Spec](https://bsonspec.org/)

### BSON serialization and deserialiation

**`new BSON()`** - Creates a new BSON serializer/deserializer you can use to serialize and deserialize BSON.

#### BSON.serialize

The BSON `serialize` method takes a JavaScript object and an optional options object and returns a Node.js Buffer.

  * `BSON.serialize(object, options)`
    * @param {Object} object the JavaScript object to serialize.
    * @param {Boolean} [options.checkKeys=false] the serializer will check if keys are valid.
    * @param {Boolean} [options.serializeFunctions=false] serialize the JavaScript functions.
    * @param {Boolean} [options.ignoreUndefined=true]
    * @return {Buffer} returns a Buffer instance.

#### BSON.serializeWithBufferAndIndex

The BSON `serializeWithBufferAndIndex` method takes an object, a target buffer instance and an optional options object and returns the end serialization index in the final buffer.

  * `BSON.serializeWithBufferAndIndex(object, buffer, options)`
    * @param {Object} object the JavaScript object to serialize.
    * @param {Buffer} buffer the Buffer you pre-allocated to store the serialized BSON object.
    * @param {Boolean} [options.checkKeys=false] the serializer will check if keys are valid.
    * @param {Boolean} [options.serializeFunctions=false] serialize the JavaScript functions.
    * @param {Boolean} [options.ignoreUndefined=true] ignore undefined fields.
    * @param {Number} [options.index=0] the index in the buffer where we wish to start serializing into.
    * @return {Number} returns the index pointing to the last written byte in the buffer.

#### BSON.calculateObjectSize

The BSON `calculateObjectSize` method takes a JavaScript object and an optional options object and returns the size of the BSON object.

  * `BSON.calculateObjectSize(object, options)`
    * @param {Object} object the JavaScript object to serialize.
    * @param {Boolean} [options.serializeFunctions=false] serialize the JavaScript functions.
    * @param {Boolean} [options.ignoreUndefined=true]
    * @return {Buffer} returns a Buffer instance.

#### BSON.deserialize

The BSON `deserialize` method takes a Node.js Buffer and an optional options object and returns a deserialized JavaScript object.

  * `BSON.deserialize(buffer, options)`
    * @param {Object} [options.evalFunctions=false] evaluate functions in the BSON document scoped to the object deserialized.
    * @param {Object} [options.cacheFunctions=false] cache evaluated functions for reuse.
    * @param {Object} [options.cacheFunctionsCrc32=false] use a crc32 code for caching, otherwise use the string of the function.
    * @param {Object} [options.promoteLongs=true] when deserializing a Long will fit it into a Number if it's smaller than 53 bits
    * @param {Object} [options.promoteBuffers=false] when deserializing a Binary will return it as a Node.js Buffer instance.
    * @param {Object} [options.promoteValues=false] when deserializing will promote BSON values to their Node.js closest equivalent types.
    * @param {Object} [options.fieldsAsRaw=null] allow to specify if there what fields we wish to return as unserialized raw buffer.
    * @param {Object} [options.bsonRegExp=false] return BSON regular expressions as BSONRegExp instances.
    * @return {Object} returns the deserialized Javascript Object.

#### BSON.deserializeStream

The BSON `deserializeStream` method takes a Node.js Buffer, `startIndex` and allow more control over deserialization of a Buffer containing concatenated BSON documents.

  * `BSON.deserializeStream(buffer, startIndex, numberOfDocuments, documents, docStartIndex, options)`
    * @param {Buffer} buffer the buffer containing the serialized set of BSON documents.
    * @param {Number} startIndex the start index in the data Buffer where the deserialization is to start.
    * @param {Number} numberOfDocuments number of documents to deserialize.
    * @param {Array} documents an array where to store the deserialized documents.
    * @param {Number} docStartIndex the index in the documents array from where to start inserting documents.
    * @param {Object} [options.evalFunctions=false] evaluate functions in the BSON document scoped to the object deserialized.
    * @param {Object} [options.cacheFunctions=false] cache evaluated functions for reuse.
    * @param {Object} [options.cacheFunctionsCrc32=false] use a crc32 code for caching, otherwise use the string of the function.
    * @param {Object} [options.promoteLongs=true] when deserializing a Long will fit it into a Number if it's smaller than 53 bits
    * @param {Object} [options.promoteBuffers=false] when deserializing a Binary will return it as a Node.js Buffer instance.
    * @param {Object} [options.promoteValues=false] when deserializing will promote BSON values to their Node.js closest equivalent types.
    * @param {Object} [options.fieldsAsRaw=null] allow to specify if there what fields we wish to return as unserialized raw buffer.
    * @param {Object} [options.bsonRegExp=false] return BSON regular expressions as BSONRegExp instances.
    * @return {Number} returns the next index in the buffer after deserialization **x** numbers of documents.

## FAQ

#### Why does `undefined` get converted to `null`?

The `undefined` BSON type has been [deprecated for many years](http://bsonspec.org/spec.html), so this library has dropped support for it. Use the `ignoreUndefined` option (for example, from the [driver](http://mongodb.github.io/node-mongodb-native/2.2/api/MongoClient.html#connect) ) to instead remove `undefined` keys.

#### How do I add custom serialization logic?

This library looks for `toBSON()` functions on every path, and calls the `toBSON()` function to get the value to serialize.

```javascript
var bson = new BSON();

class CustomSerialize {
  toBSON() {
    return 42;
  }
}

const obj = { answer: new CustomSerialize() };
// "{ answer: 42 }"
console.log(bson.deserialize(bson.serialize(obj)));
```
