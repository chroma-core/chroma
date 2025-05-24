// Custom inspect property name / symbol.
var inspect = 'inspect';

var utils = require('./parser/utils');

/**
 * Machine id.
 *
 * Create a random 3-byte value (i.e. unique for this
 * process). Other drivers use a md5 of the machine id here, but
 * that would mean an asyc call to gethostname, so we don't bother.
 * @ignore
 */
var MACHINE_ID = parseInt(Math.random() * 0xffffff, 10);

// Regular expression that checks for hex value
var checkForHexRegExp = new RegExp('^[0-9a-fA-F]{24}$');

// Check if buffer exists
try {
  if (Buffer && Buffer.from) {
    var hasBufferType = true;
    inspect = require('util').inspect.custom || 'inspect';
  }
} catch (err) {
  hasBufferType = false;
}

/**
* Create a new ObjectID instance
*
* @class
* @param {(string|number)} id Can be a 24 byte hex string, 12 byte binary string or a Number.
* @property {number} generationTime The generation time of this ObjectId instance
* @return {ObjectID} instance of ObjectID.
*/
var ObjectID = function ObjectID(id) {
  // Duck-typing to support ObjectId from different npm packages
  if (id instanceof ObjectID) return id;
  if (!(this instanceof ObjectID)) return new ObjectID(id);

  this._bsontype = 'ObjectID';

  // The most common usecase (blank id, new objectId instance)
  if (id == null || typeof id === 'number') {
    // Generate a new id
    this.id = this.generate(id);
    // If we are caching the hex string
    if (ObjectID.cacheHexString) this.__id = this.toString('hex');
    // Return the object
    return;
  }

  // Check if the passed in id is valid
  var valid = ObjectID.isValid(id);

  // Throw an error if it's not a valid setup
  if (!valid && id != null) {
    throw new Error(
      'Argument passed in must be a single String of 12 bytes or a string of 24 hex characters'
    );
  } else if (valid && typeof id === 'string' && id.length === 24 && hasBufferType) {
    return new ObjectID(utils.toBuffer(id, 'hex'));
  } else if (valid && typeof id === 'string' && id.length === 24) {
    return ObjectID.createFromHexString(id);
  } else if (id != null && id.length === 12) {
    // assume 12 byte string
    this.id = id;
  } else if (id != null && typeof id.toHexString === 'function') {
    // Duck-typing to support ObjectId from different npm packages
    return id;
  } else {
    throw new Error(
      'Argument passed in must be a single String of 12 bytes or a string of 24 hex characters'
    );
  }

  if (ObjectID.cacheHexString) this.__id = this.toString('hex');
};

// Allow usage of ObjectId as well as ObjectID
// var ObjectId = ObjectID;

// Precomputed hex table enables speedy hex string conversion
var hexTable = [];
for (var i = 0; i < 256; i++) {
  hexTable[i] = (i <= 15 ? '0' : '') + i.toString(16);
}

/**
* Return the ObjectID id as a 24 byte hex string representation
*
* @method
* @return {string} return the 24 byte hex string representation.
*/
ObjectID.prototype.toHexString = function() {
  if (ObjectID.cacheHexString && this.__id) return this.__id;

  var hexString = '';
  if (!this.id || !this.id.length) {
    throw new Error(
      'invalid ObjectId, ObjectId.id must be either a string or a Buffer, but is [' +
        JSON.stringify(this.id) +
        ']'
    );
  }

  if (this.id instanceof _Buffer) {
    hexString = convertToHex(this.id);
    if (ObjectID.cacheHexString) this.__id = hexString;
    return hexString;
  }

  for (var i = 0; i < this.id.length; i++) {
    hexString += hexTable[this.id.charCodeAt(i)];
  }

  if (ObjectID.cacheHexString) this.__id = hexString;
  return hexString;
};

/**
* Update the ObjectID index used in generating new ObjectID's on the driver
*
* @method
* @return {number} returns next index value.
* @ignore
*/
ObjectID.prototype.get_inc = function() {
  return (ObjectID.index = (ObjectID.index + 1) % 0xffffff);
};

/**
* Update the ObjectID index used in generating new ObjectID's on the driver
*
* @method
* @return {number} returns next index value.
* @ignore
*/
ObjectID.prototype.getInc = function() {
  return this.get_inc();
};

/**
* Generate a 12 byte id buffer used in ObjectID's
*
* @method
* @param {number} [time] optional parameter allowing to pass in a second based timestamp.
* @return {Buffer} return the 12 byte id buffer string.
*/
ObjectID.prototype.generate = function(time) {
  if ('number' !== typeof time) {
    time = ~~(Date.now() / 1000);
  }

  // Use pid
  var pid =
    (typeof process === 'undefined' || process.pid === 1
      ? Math.floor(Math.random() * 100000)
      : process.pid) % 0xffff;
  var inc = this.get_inc();
  // Buffer used
  var buffer = utils.allocBuffer(12);
  // Encode time
  buffer[3] = time & 0xff;
  buffer[2] = (time >> 8) & 0xff;
  buffer[1] = (time >> 16) & 0xff;
  buffer[0] = (time >> 24) & 0xff;
  // Encode machine
  buffer[6] = MACHINE_ID & 0xff;
  buffer[5] = (MACHINE_ID >> 8) & 0xff;
  buffer[4] = (MACHINE_ID >> 16) & 0xff;
  // Encode pid
  buffer[8] = pid & 0xff;
  buffer[7] = (pid >> 8) & 0xff;
  // Encode index
  buffer[11] = inc & 0xff;
  buffer[10] = (inc >> 8) & 0xff;
  buffer[9] = (inc >> 16) & 0xff;
  // Return the buffer
  return buffer;
};

/**
* Converts the id into a 24 byte hex string for printing
*
* @param {String} format The Buffer toString format parameter.
* @return {String} return the 24 byte hex string representation.
* @ignore
*/
ObjectID.prototype.toString = function(format) {
  // Is the id a buffer then use the buffer toString method to return the format
  if (this.id && this.id.copy) {
    return this.id.toString(typeof format === 'string' ? format : 'hex');
  }

  // if(this.buffer )
  return this.toHexString();
};

/**
* Converts to a string representation of this Id.
*
* @return {String} return the 24 byte hex string representation.
* @ignore
*/
ObjectID.prototype[inspect] = ObjectID.prototype.toString;

/**
* Converts to its JSON representation.
*
* @return {String} return the 24 byte hex string representation.
* @ignore
*/
ObjectID.prototype.toJSON = function() {
  return this.toHexString();
};

/**
* Compares the equality of this ObjectID with `otherID`.
*
* @method
* @param {object} otherID ObjectID instance to compare against.
* @return {boolean} the result of comparing two ObjectID's
*/
ObjectID.prototype.equals = function equals(otherId) {
  // var id;

  if (otherId instanceof ObjectID) {
    return this.toString() === otherId.toString();
  } else if (
    typeof otherId === 'string' &&
    ObjectID.isValid(otherId) &&
    otherId.length === 12 &&
    this.id instanceof _Buffer
  ) {
    return otherId === this.id.toString('binary');
  } else if (typeof otherId === 'string' && ObjectID.isValid(otherId) && otherId.length === 24) {
    return otherId.toLowerCase() === this.toHexString();
  } else if (typeof otherId === 'string' && ObjectID.isValid(otherId) && otherId.length === 12) {
    return otherId === this.id;
  } else if (otherId != null && (otherId instanceof ObjectID || otherId.toHexString)) {
    return otherId.toHexString() === this.toHexString();
  } else {
    return false;
  }
};

/**
* Returns the generation date (accurate up to the second) that this ID was generated.
*
* @method
* @return {date} the generation date
*/
ObjectID.prototype.getTimestamp = function() {
  var timestamp = new Date();
  var time = this.id[3] | (this.id[2] << 8) | (this.id[1] << 16) | (this.id[0] << 24);
  timestamp.setTime(Math.floor(time) * 1000);
  return timestamp;
};

/**
* @ignore
*/
ObjectID.index = ~~(Math.random() * 0xffffff);

/**
* @ignore
*/
ObjectID.createPk = function createPk() {
  return new ObjectID();
};

/**
* Creates an ObjectID from a second based number, with the rest of the ObjectID zeroed out. Used for comparisons or sorting the ObjectID.
*
* @method
* @param {number} time an integer number representing a number of seconds.
* @return {ObjectID} return the created ObjectID
*/
ObjectID.createFromTime = function createFromTime(time) {
  var buffer = utils.toBuffer([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  // Encode time into first 4 bytes
  buffer[3] = time & 0xff;
  buffer[2] = (time >> 8) & 0xff;
  buffer[1] = (time >> 16) & 0xff;
  buffer[0] = (time >> 24) & 0xff;
  // Return the new objectId
  return new ObjectID(buffer);
};

// Lookup tables
//var encodeLookup = '0123456789abcdef'.split('');
var decodeLookup = [];
i = 0;
while (i < 10) decodeLookup[0x30 + i] = i++;
while (i < 16) decodeLookup[0x41 - 10 + i] = decodeLookup[0x61 - 10 + i] = i++;

var _Buffer = Buffer;
var convertToHex = function(bytes) {
  return bytes.toString('hex');
};

/**
* Creates an ObjectID from a hex string representation of an ObjectID.
*
* @method
* @param {string} hexString create a ObjectID from a passed in 24 byte hexstring.
* @return {ObjectID} return the created ObjectID
*/
ObjectID.createFromHexString = function createFromHexString(string) {
  // Throw an error if it's not a valid setup
  if (typeof string === 'undefined' || (string != null && string.length !== 24)) {
    throw new Error(
      'Argument passed in must be a single String of 12 bytes or a string of 24 hex characters'
    );
  }

  // Use Buffer.from method if available
  if (hasBufferType) return new ObjectID(utils.toBuffer(string, 'hex'));

  // Calculate lengths
  var array = new _Buffer(12);
  var n = 0;
  var i = 0;

  while (i < 24) {
    array[n++] = (decodeLookup[string.charCodeAt(i++)] << 4) | decodeLookup[string.charCodeAt(i++)];
  }

  return new ObjectID(array);
};

/**
* Checks if a value is a valid bson ObjectId
*
* @method
* @return {boolean} return true if the value is a valid bson ObjectId, return false otherwise.
*/
ObjectID.isValid = function isValid(id) {
  if (id == null) return false;

  if (typeof id === 'number') {
    return true;
  }

  if (typeof id === 'string') {
    return id.length === 12 || (id.length === 24 && checkForHexRegExp.test(id));
  }

  if (id instanceof ObjectID) {
    return true;
  }

  if (id instanceof _Buffer) {
    return true;
  }

  // Duck-Typing detection of ObjectId like objects
  if (
      typeof id.toHexString === 'function' &&
      (id.id instanceof _Buffer || typeof id.id === 'string')
  ) {
    return id.id.length === 12 || (id.id.length === 24 && checkForHexRegExp.test(id.id));
  }

  return false;
};

/**
* @ignore
*/
Object.defineProperty(ObjectID.prototype, 'generationTime', {
  enumerable: true,
  get: function() {
    return this.id[3] | (this.id[2] << 8) | (this.id[1] << 16) | (this.id[0] << 24);
  },
  set: function(value) {
    // Encode time into first 4 bytes
    this.id[3] = value & 0xff;
    this.id[2] = (value >> 8) & 0xff;
    this.id[1] = (value >> 16) & 0xff;
    this.id[0] = (value >> 24) & 0xff;
  }
});

/**
 * Expose.
 */
module.exports = ObjectID;
module.exports.ObjectID = ObjectID;
module.exports.ObjectId = ObjectID;
