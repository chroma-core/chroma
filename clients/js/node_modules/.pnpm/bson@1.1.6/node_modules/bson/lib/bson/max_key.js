/**
 * A class representation of the BSON MaxKey type.
 *
 * @class
 * @return {MaxKey} A MaxKey instance
 */
function MaxKey() {
  if (!(this instanceof MaxKey)) return new MaxKey();

  this._bsontype = 'MaxKey';
}

module.exports = MaxKey;
module.exports.MaxKey = MaxKey;
