/**
 * A class representation of the BSON MinKey type.
 *
 * @class
 * @return {MinKey} A MinKey instance
 */
function MinKey() {
  if (!(this instanceof MinKey)) return new MinKey();

  this._bsontype = 'MinKey';
}

module.exports = MinKey;
module.exports.MinKey = MinKey;
