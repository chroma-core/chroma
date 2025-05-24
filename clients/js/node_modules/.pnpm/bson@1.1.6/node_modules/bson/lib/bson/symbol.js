// Custom inspect property name / symbol.
var inspect = Buffer ? require('util').inspect.custom || 'inspect' : 'inspect';

/**
 * A class representation of the BSON Symbol type.
 *
 * @class
 * @deprecated
 * @param {string} value the string representing the symbol.
 * @return {Symbol}
 */
function Symbol(value) {
  if (!(this instanceof Symbol)) return new Symbol(value);
  this._bsontype = 'Symbol';
  this.value = value;
}

/**
 * Access the wrapped string value.
 *
 * @method
 * @return {String} returns the wrapped string.
 */
Symbol.prototype.valueOf = function() {
  return this.value;
};

/**
 * @ignore
 */
Symbol.prototype.toString = function() {
  return this.value;
};

/**
 * @ignore
 */
Symbol.prototype[inspect] = function() {
  return this.value;
};

/**
 * @ignore
 */
Symbol.prototype.toJSON = function() {
  return this.value;
};

module.exports = Symbol;
module.exports.Symbol = Symbol;
