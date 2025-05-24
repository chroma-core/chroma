/**
 * A class representation of a BSON Int32 type.
 *
 * @class
 * @param {number} value the number we want to represent as an int32.
 * @return {Int32}
 */
var Int32 = function(value) {
  if (!(this instanceof Int32)) return new Int32(value);

  this._bsontype = 'Int32';
  this.value = value;
};

/**
 * Access the number value.
 *
 * @method
 * @return {number} returns the wrapped int32 number.
 */
Int32.prototype.valueOf = function() {
  return this.value;
};

/**
 * @ignore
 */
Int32.prototype.toJSON = function() {
  return this.value;
};

module.exports = Int32;
module.exports.Int32 = Int32;
