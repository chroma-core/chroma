/**
 * A class representation of the BSON RegExp type.
 *
 * @class
 * @return {BSONRegExp} A MinKey instance
 */
function BSONRegExp(pattern, options) {
  if (!(this instanceof BSONRegExp)) return new BSONRegExp();

  // Execute
  this._bsontype = 'BSONRegExp';
  this.pattern = pattern || '';
  this.options = options || '';

  // Validate options
  for (var i = 0; i < this.options.length; i++) {
    if (
      !(
        this.options[i] === 'i' ||
        this.options[i] === 'm' ||
        this.options[i] === 'x' ||
        this.options[i] === 'l' ||
        this.options[i] === 's' ||
        this.options[i] === 'u'
      )
    ) {
      throw new Error('the regular expression options [' + this.options[i] + '] is not supported');
    }
  }
}

module.exports = BSONRegExp;
module.exports.BSONRegExp = BSONRegExp;
