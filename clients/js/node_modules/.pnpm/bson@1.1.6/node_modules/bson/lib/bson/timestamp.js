// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright 2009 Google Inc. All Rights Reserved

/**
 * This type is for INTERNAL use in MongoDB only and should not be used in applications.
 * The appropriate corresponding type is the JavaScript Date type.
 * 
 * Defines a Timestamp class for representing a 64-bit two's-complement
 * integer value, which faithfully simulates the behavior of a Java "Timestamp". This
 * implementation is derived from TimestampLib in GWT.
 *
 * Constructs a 64-bit two's-complement integer, given its low and high 32-bit
 * values as *signed* integers.  See the from* functions below for more
 * convenient ways of constructing Timestamps.
 *
 * The internal representation of a Timestamp is the two given signed, 32-bit values.
 * We use 32-bit pieces because these are the size of integers on which
 * Javascript performs bit-operations.  For operations like addition and
 * multiplication, we split each number into 16-bit pieces, which can easily be
 * multiplied within Javascript's floating-point representation without overflow
 * or change in sign.
 *
 * In the algorithms below, we frequently reduce the negative case to the
 * positive case by negating the input(s) and then post-processing the result.
 * Note that we must ALWAYS check specially whether those values are MIN_VALUE
 * (-2^63) because -MIN_VALUE == MIN_VALUE (since 2^63 cannot be represented as
 * a positive number, it overflows back into a negative).  Not handling this
 * case would often result in infinite recursion.
 *
 * @class
 * @param {number} low  the low (signed) 32 bits of the Timestamp.
 * @param {number} high the high (signed) 32 bits of the Timestamp.
 */
function Timestamp(low, high) {
  if (!(this instanceof Timestamp)) return new Timestamp(low, high);
  this._bsontype = 'Timestamp';
  /**
   * @type {number}
   * @ignore
   */
  this.low_ = low | 0; // force into 32 signed bits.

  /**
   * @type {number}
   * @ignore
   */
  this.high_ = high | 0; // force into 32 signed bits.
}

/**
 * Return the int value.
 *
 * @return {number} the value, assuming it is a 32-bit integer.
 */
Timestamp.prototype.toInt = function() {
  return this.low_;
};

/**
 * Return the Number value.
 *
 * @method
 * @return {number} the closest floating-point representation to this value.
 */
Timestamp.prototype.toNumber = function() {
  return this.high_ * Timestamp.TWO_PWR_32_DBL_ + this.getLowBitsUnsigned();
};

/**
 * Return the JSON value.
 *
 * @method
 * @return {string} the JSON representation.
 */
Timestamp.prototype.toJSON = function() {
  return this.toString();
};

/**
 * Return the String value.
 *
 * @method
 * @param {number} [opt_radix] the radix in which the text should be written.
 * @return {string} the textual representation of this value.
 */
Timestamp.prototype.toString = function(opt_radix) {
  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (this.isZero()) {
    return '0';
  }

  if (this.isNegative()) {
    if (this.equals(Timestamp.MIN_VALUE)) {
      // We need to change the Timestamp value before it can be negated, so we remove
      // the bottom-most digit in this base and then recurse to do the rest.
      var radixTimestamp = Timestamp.fromNumber(radix);
      var div = this.div(radixTimestamp);
      var rem = div.multiply(radixTimestamp).subtract(this);
      return div.toString(radix) + rem.toInt().toString(radix);
    } else {
      return '-' + this.negate().toString(radix);
    }
  }

  // Do several (6) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 6));

  rem = this;
  var result = '';

  while (!rem.isZero()) {
    var remDiv = rem.div(radixToPower);
    var intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
    var digits = intval.toString(radix);

    rem = remDiv;
    if (rem.isZero()) {
      return digits + result;
    } else {
      while (digits.length < 6) {
        digits = '0' + digits;
      }
      result = '' + digits + result;
    }
  }
};

/**
 * Return the high 32-bits value.
 *
 * @method
 * @return {number} the high 32-bits as a signed value.
 */
Timestamp.prototype.getHighBits = function() {
  return this.high_;
};

/**
 * Return the low 32-bits value.
 *
 * @method
 * @return {number} the low 32-bits as a signed value.
 */
Timestamp.prototype.getLowBits = function() {
  return this.low_;
};

/**
 * Return the low unsigned 32-bits value.
 *
 * @method
 * @return {number} the low 32-bits as an unsigned value.
 */
Timestamp.prototype.getLowBitsUnsigned = function() {
  return this.low_ >= 0 ? this.low_ : Timestamp.TWO_PWR_32_DBL_ + this.low_;
};

/**
 * Returns the number of bits needed to represent the absolute value of this Timestamp.
 *
 * @method
 * @return {number} Returns the number of bits needed to represent the absolute value of this Timestamp.
 */
Timestamp.prototype.getNumBitsAbs = function() {
  if (this.isNegative()) {
    if (this.equals(Timestamp.MIN_VALUE)) {
      return 64;
    } else {
      return this.negate().getNumBitsAbs();
    }
  } else {
    var val = this.high_ !== 0 ? this.high_ : this.low_;
    for (var bit = 31; bit > 0; bit--) {
      if ((val & (1 << bit)) !== 0) {
        break;
      }
    }
    return this.high_ !== 0 ? bit + 33 : bit + 1;
  }
};

/**
 * Return whether this value is zero.
 *
 * @method
 * @return {boolean} whether this value is zero.
 */
Timestamp.prototype.isZero = function() {
  return this.high_ === 0 && this.low_ === 0;
};

/**
 * Return whether this value is negative.
 *
 * @method
 * @return {boolean} whether this value is negative.
 */
Timestamp.prototype.isNegative = function() {
  return this.high_ < 0;
};

/**
 * Return whether this value is odd.
 *
 * @method
 * @return {boolean} whether this value is odd.
 */
Timestamp.prototype.isOdd = function() {
  return (this.low_ & 1) === 1;
};

/**
 * Return whether this Timestamp equals the other
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp equals the other
 */
Timestamp.prototype.equals = function(other) {
  return this.high_ === other.high_ && this.low_ === other.low_;
};

/**
 * Return whether this Timestamp does not equal the other.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp does not equal the other.
 */
Timestamp.prototype.notEquals = function(other) {
  return this.high_ !== other.high_ || this.low_ !== other.low_;
};

/**
 * Return whether this Timestamp is less than the other.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp is less than the other.
 */
Timestamp.prototype.lessThan = function(other) {
  return this.compare(other) < 0;
};

/**
 * Return whether this Timestamp is less than or equal to the other.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp is less than or equal to the other.
 */
Timestamp.prototype.lessThanOrEqual = function(other) {
  return this.compare(other) <= 0;
};

/**
 * Return whether this Timestamp is greater than the other.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp is greater than the other.
 */
Timestamp.prototype.greaterThan = function(other) {
  return this.compare(other) > 0;
};

/**
 * Return whether this Timestamp is greater than or equal to the other.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} whether this Timestamp is greater than or equal to the other.
 */
Timestamp.prototype.greaterThanOrEqual = function(other) {
  return this.compare(other) >= 0;
};

/**
 * Compares this Timestamp with the given one.
 *
 * @method
 * @param {Timestamp} other Timestamp to compare against.
 * @return {boolean} 0 if they are the same, 1 if the this is greater, and -1 if the given one is greater.
 */
Timestamp.prototype.compare = function(other) {
  if (this.equals(other)) {
    return 0;
  }

  var thisNeg = this.isNegative();
  var otherNeg = other.isNegative();
  if (thisNeg && !otherNeg) {
    return -1;
  }
  if (!thisNeg && otherNeg) {
    return 1;
  }

  // at this point, the signs are the same, so subtraction will not overflow
  if (this.subtract(other).isNegative()) {
    return -1;
  } else {
    return 1;
  }
};

/**
 * The negation of this value.
 *
 * @method
 * @return {Timestamp} the negation of this value.
 */
Timestamp.prototype.negate = function() {
  if (this.equals(Timestamp.MIN_VALUE)) {
    return Timestamp.MIN_VALUE;
  } else {
    return this.not().add(Timestamp.ONE);
  }
};

/**
 * Returns the sum of this and the given Timestamp.
 *
 * @method
 * @param {Timestamp} other Timestamp to add to this one.
 * @return {Timestamp} the sum of this and the given Timestamp.
 */
Timestamp.prototype.add = function(other) {
  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xffff;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xffff;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xffff;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xffff;

  var c48 = 0,
    c32 = 0,
    c16 = 0,
    c00 = 0;
  c00 += a00 + b00;
  c16 += c00 >>> 16;
  c00 &= 0xffff;
  c16 += a16 + b16;
  c32 += c16 >>> 16;
  c16 &= 0xffff;
  c32 += a32 + b32;
  c48 += c32 >>> 16;
  c32 &= 0xffff;
  c48 += a48 + b48;
  c48 &= 0xffff;
  return Timestamp.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns the difference of this and the given Timestamp.
 *
 * @method
 * @param {Timestamp} other Timestamp to subtract from this.
 * @return {Timestamp} the difference of this and the given Timestamp.
 */
Timestamp.prototype.subtract = function(other) {
  return this.add(other.negate());
};

/**
 * Returns the product of this and the given Timestamp.
 *
 * @method
 * @param {Timestamp} other Timestamp to multiply with this.
 * @return {Timestamp} the product of this and the other.
 */
Timestamp.prototype.multiply = function(other) {
  if (this.isZero()) {
    return Timestamp.ZERO;
  } else if (other.isZero()) {
    return Timestamp.ZERO;
  }

  if (this.equals(Timestamp.MIN_VALUE)) {
    return other.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
  } else if (other.equals(Timestamp.MIN_VALUE)) {
    return this.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().multiply(other.negate());
    } else {
      return this.negate()
        .multiply(other)
        .negate();
    }
  } else if (other.isNegative()) {
    return this.multiply(other.negate()).negate();
  }

  // If both Timestamps are small, use float multiplication
  if (this.lessThan(Timestamp.TWO_PWR_24_) && other.lessThan(Timestamp.TWO_PWR_24_)) {
    return Timestamp.fromNumber(this.toNumber() * other.toNumber());
  }

  // Divide each Timestamp into 4 chunks of 16 bits, and then add up 4x4 products.
  // We can skip products that would overflow.

  var a48 = this.high_ >>> 16;
  var a32 = this.high_ & 0xffff;
  var a16 = this.low_ >>> 16;
  var a00 = this.low_ & 0xffff;

  var b48 = other.high_ >>> 16;
  var b32 = other.high_ & 0xffff;
  var b16 = other.low_ >>> 16;
  var b00 = other.low_ & 0xffff;

  var c48 = 0,
    c32 = 0,
    c16 = 0,
    c00 = 0;
  c00 += a00 * b00;
  c16 += c00 >>> 16;
  c00 &= 0xffff;
  c16 += a16 * b00;
  c32 += c16 >>> 16;
  c16 &= 0xffff;
  c16 += a00 * b16;
  c32 += c16 >>> 16;
  c16 &= 0xffff;
  c32 += a32 * b00;
  c48 += c32 >>> 16;
  c32 &= 0xffff;
  c32 += a16 * b16;
  c48 += c32 >>> 16;
  c32 &= 0xffff;
  c32 += a00 * b32;
  c48 += c32 >>> 16;
  c32 &= 0xffff;
  c48 += a48 * b00 + a32 * b16 + a16 * b32 + a00 * b48;
  c48 &= 0xffff;
  return Timestamp.fromBits((c16 << 16) | c00, (c48 << 16) | c32);
};

/**
 * Returns this Timestamp divided by the given one.
 *
 * @method
 * @param {Timestamp} other Timestamp by which to divide.
 * @return {Timestamp} this Timestamp divided by the given one.
 */
Timestamp.prototype.div = function(other) {
  if (other.isZero()) {
    throw Error('division by zero');
  } else if (this.isZero()) {
    return Timestamp.ZERO;
  }

  if (this.equals(Timestamp.MIN_VALUE)) {
    if (other.equals(Timestamp.ONE) || other.equals(Timestamp.NEG_ONE)) {
      return Timestamp.MIN_VALUE; // recall that -MIN_VALUE == MIN_VALUE
    } else if (other.equals(Timestamp.MIN_VALUE)) {
      return Timestamp.ONE;
    } else {
      // At this point, we have |other| >= 2, so |this/other| < |MIN_VALUE|.
      var halfThis = this.shiftRight(1);
      var approx = halfThis.div(other).shiftLeft(1);
      if (approx.equals(Timestamp.ZERO)) {
        return other.isNegative() ? Timestamp.ONE : Timestamp.NEG_ONE;
      } else {
        var rem = this.subtract(other.multiply(approx));
        var result = approx.add(rem.div(other));
        return result;
      }
    }
  } else if (other.equals(Timestamp.MIN_VALUE)) {
    return Timestamp.ZERO;
  }

  if (this.isNegative()) {
    if (other.isNegative()) {
      return this.negate().div(other.negate());
    } else {
      return this.negate()
        .div(other)
        .negate();
    }
  } else if (other.isNegative()) {
    return this.div(other.negate()).negate();
  }

  // Repeat the following until the remainder is less than other:  find a
  // floating-point that approximates remainder / other *from below*, add this
  // into the result, and subtract it from the remainder.  It is critical that
  // the approximate value is less than or equal to the real value so that the
  // remainder never becomes negative.
  var res = Timestamp.ZERO;
  rem = this;
  while (rem.greaterThanOrEqual(other)) {
    // Approximate the result of division. This may be a little greater or
    // smaller than the actual value.
    approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));

    // We will tweak the approximate result by changing it in the 48-th digit or
    // the smallest non-fractional digit, whichever is larger.
    var log2 = Math.ceil(Math.log(approx) / Math.LN2);
    var delta = log2 <= 48 ? 1 : Math.pow(2, log2 - 48);

    // Decrease the approximation until it is smaller than the remainder.  Note
    // that if it is too large, the product overflows and is negative.
    var approxRes = Timestamp.fromNumber(approx);
    var approxRem = approxRes.multiply(other);
    while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
      approx -= delta;
      approxRes = Timestamp.fromNumber(approx);
      approxRem = approxRes.multiply(other);
    }

    // We know the answer can't be zero... and actually, zero would cause
    // infinite recursion since we would make no progress.
    if (approxRes.isZero()) {
      approxRes = Timestamp.ONE;
    }

    res = res.add(approxRes);
    rem = rem.subtract(approxRem);
  }
  return res;
};

/**
 * Returns this Timestamp modulo the given one.
 *
 * @method
 * @param {Timestamp} other Timestamp by which to mod.
 * @return {Timestamp} this Timestamp modulo the given one.
 */
Timestamp.prototype.modulo = function(other) {
  return this.subtract(this.div(other).multiply(other));
};

/**
 * The bitwise-NOT of this value.
 *
 * @method
 * @return {Timestamp} the bitwise-NOT of this value.
 */
Timestamp.prototype.not = function() {
  return Timestamp.fromBits(~this.low_, ~this.high_);
};

/**
 * Returns the bitwise-AND of this Timestamp and the given one.
 *
 * @method
 * @param {Timestamp} other the Timestamp with which to AND.
 * @return {Timestamp} the bitwise-AND of this and the other.
 */
Timestamp.prototype.and = function(other) {
  return Timestamp.fromBits(this.low_ & other.low_, this.high_ & other.high_);
};

/**
 * Returns the bitwise-OR of this Timestamp and the given one.
 *
 * @method
 * @param {Timestamp} other the Timestamp with which to OR.
 * @return {Timestamp} the bitwise-OR of this and the other.
 */
Timestamp.prototype.or = function(other) {
  return Timestamp.fromBits(this.low_ | other.low_, this.high_ | other.high_);
};

/**
 * Returns the bitwise-XOR of this Timestamp and the given one.
 *
 * @method
 * @param {Timestamp} other the Timestamp with which to XOR.
 * @return {Timestamp} the bitwise-XOR of this and the other.
 */
Timestamp.prototype.xor = function(other) {
  return Timestamp.fromBits(this.low_ ^ other.low_, this.high_ ^ other.high_);
};

/**
 * Returns this Timestamp with bits shifted to the left by the given amount.
 *
 * @method
 * @param {number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the left by the given amount.
 */
Timestamp.prototype.shiftLeft = function(numBits) {
  numBits &= 63;
  if (numBits === 0) {
    return this;
  } else {
    var low = this.low_;
    if (numBits < 32) {
      var high = this.high_;
      return Timestamp.fromBits(low << numBits, (high << numBits) | (low >>> (32 - numBits)));
    } else {
      return Timestamp.fromBits(0, low << (numBits - 32));
    }
  }
};

/**
 * Returns this Timestamp with bits shifted to the right by the given amount.
 *
 * @method
 * @param {number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the right by the given amount.
 */
Timestamp.prototype.shiftRight = function(numBits) {
  numBits &= 63;
  if (numBits === 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Timestamp.fromBits((low >>> numBits) | (high << (32 - numBits)), high >> numBits);
    } else {
      return Timestamp.fromBits(high >> (numBits - 32), high >= 0 ? 0 : -1);
    }
  }
};

/**
 * Returns this Timestamp with bits shifted to the right by the given amount, with the new top bits matching the current sign bit.
 *
 * @method
 * @param {number} numBits the number of bits by which to shift.
 * @return {Timestamp} this shifted to the right by the given amount, with zeros placed into the new leading bits.
 */
Timestamp.prototype.shiftRightUnsigned = function(numBits) {
  numBits &= 63;
  if (numBits === 0) {
    return this;
  } else {
    var high = this.high_;
    if (numBits < 32) {
      var low = this.low_;
      return Timestamp.fromBits((low >>> numBits) | (high << (32 - numBits)), high >>> numBits);
    } else if (numBits === 32) {
      return Timestamp.fromBits(high, 0);
    } else {
      return Timestamp.fromBits(high >>> (numBits - 32), 0);
    }
  }
};

/**
 * Returns a Timestamp representing the given (32-bit) integer value.
 *
 * @method
 * @param {number} value the 32-bit integer in question.
 * @return {Timestamp} the corresponding Timestamp value.
 */
Timestamp.fromInt = function(value) {
  if (-128 <= value && value < 128) {
    var cachedObj = Timestamp.INT_CACHE_[value];
    if (cachedObj) {
      return cachedObj;
    }
  }

  var obj = new Timestamp(value | 0, value < 0 ? -1 : 0);
  if (-128 <= value && value < 128) {
    Timestamp.INT_CACHE_[value] = obj;
  }
  return obj;
};

/**
 * Returns a Timestamp representing the given value, provided that it is a finite number. Otherwise, zero is returned.
 *
 * @method
 * @param {number} value the number in question.
 * @return {Timestamp} the corresponding Timestamp value.
 */
Timestamp.fromNumber = function(value) {
  if (isNaN(value) || !isFinite(value)) {
    return Timestamp.ZERO;
  } else if (value <= -Timestamp.TWO_PWR_63_DBL_) {
    return Timestamp.MIN_VALUE;
  } else if (value + 1 >= Timestamp.TWO_PWR_63_DBL_) {
    return Timestamp.MAX_VALUE;
  } else if (value < 0) {
    return Timestamp.fromNumber(-value).negate();
  } else {
    return new Timestamp(
      (value % Timestamp.TWO_PWR_32_DBL_) | 0,
      (value / Timestamp.TWO_PWR_32_DBL_) | 0
    );
  }
};

/**
 * Returns a Timestamp representing the 64-bit integer that comes by concatenating the given high and low bits. Each is assumed to use 32 bits.
 *
 * @method
 * @param {number} lowBits the low 32-bits.
 * @param {number} highBits the high 32-bits.
 * @return {Timestamp} the corresponding Timestamp value.
 */
Timestamp.fromBits = function(lowBits, highBits) {
  return new Timestamp(lowBits, highBits);
};

/**
 * Returns a Timestamp representation of the given string, written using the given radix.
 *
 * @method
 * @param {string} str the textual representation of the Timestamp.
 * @param {number} opt_radix the radix in which the text is written.
 * @return {Timestamp} the corresponding Timestamp value.
 */
Timestamp.fromString = function(str, opt_radix) {
  if (str.length === 0) {
    throw Error('number format error: empty string');
  }

  var radix = opt_radix || 10;
  if (radix < 2 || 36 < radix) {
    throw Error('radix out of range: ' + radix);
  }

  if (str.charAt(0) === '-') {
    return Timestamp.fromString(str.substring(1), radix).negate();
  } else if (str.indexOf('-') >= 0) {
    throw Error('number format error: interior "-" character: ' + str);
  }

  // Do several (8) digits each time through the loop, so as to
  // minimize the calls to the very expensive emulated div.
  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 8));

  var result = Timestamp.ZERO;
  for (var i = 0; i < str.length; i += 8) {
    var size = Math.min(8, str.length - i);
    var value = parseInt(str.substring(i, i + size), radix);
    if (size < 8) {
      var power = Timestamp.fromNumber(Math.pow(radix, size));
      result = result.multiply(power).add(Timestamp.fromNumber(value));
    } else {
      result = result.multiply(radixToPower);
      result = result.add(Timestamp.fromNumber(value));
    }
  }
  return result;
};

// NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
// from* methods on which they depend.

/**
 * A cache of the Timestamp representations of small integer values.
 * @type {Object}
 * @ignore
 */
Timestamp.INT_CACHE_ = {};

// NOTE: the compiler should inline these constant values below and then remove
// these variables, so there should be no runtime penalty for these.

/**
 * Number used repeated below in calculations.  This must appear before the
 * first call to any from* function below.
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_16_DBL_ = 1 << 16;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_24_DBL_ = 1 << 24;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_32_DBL_ = Timestamp.TWO_PWR_16_DBL_ * Timestamp.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_31_DBL_ = Timestamp.TWO_PWR_32_DBL_ / 2;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_48_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_16_DBL_;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_64_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_32_DBL_;

/**
 * @type {number}
 * @ignore
 */
Timestamp.TWO_PWR_63_DBL_ = Timestamp.TWO_PWR_64_DBL_ / 2;

/** @type {Timestamp} */
Timestamp.ZERO = Timestamp.fromInt(0);

/** @type {Timestamp} */
Timestamp.ONE = Timestamp.fromInt(1);

/** @type {Timestamp} */
Timestamp.NEG_ONE = Timestamp.fromInt(-1);

/** @type {Timestamp} */
Timestamp.MAX_VALUE = Timestamp.fromBits(0xffffffff | 0, 0x7fffffff | 0);

/** @type {Timestamp} */
Timestamp.MIN_VALUE = Timestamp.fromBits(0, 0x80000000 | 0);

/**
 * @type {Timestamp}
 * @ignore
 */
Timestamp.TWO_PWR_24_ = Timestamp.fromInt(1 << 24);

/**
 * Expose.
 */
module.exports = Timestamp;
module.exports.Timestamp = Timestamp;
