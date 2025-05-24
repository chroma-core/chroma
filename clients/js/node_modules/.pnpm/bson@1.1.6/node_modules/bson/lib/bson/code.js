/**
 * A class representation of the BSON Code type.
 *
 * @class
 * @param {(string|function)} code a string or function.
 * @param {Object} [scope] an optional scope for the function.
 * @return {Code}
 */
var Code = function Code(code, scope) {
  if (!(this instanceof Code)) return new Code(code, scope);
  this._bsontype = 'Code';
  this.code = code;
  this.scope = scope;
};

/**
 * @ignore
 */
Code.prototype.toJSON = function() {
  return { scope: this.scope, code: this.code };
};

module.exports = Code;
module.exports.Code = Code;
