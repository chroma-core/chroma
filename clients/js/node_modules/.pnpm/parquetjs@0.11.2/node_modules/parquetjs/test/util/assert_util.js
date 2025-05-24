'use strict';
const chai = require('chai');
const assert = chai.assert;

const EPSILON_DEFAULT = 0.01;

exports.assertArrayEqualEpsilon = function(a, b, e) {
  if (!e) {
    e = EPSILON_DEFAULT;
  }

  assert.equal(a.length, b.length);
  for (let i = 0; i < a.length; ++i) {
    assert(Math.abs(a[i] - b[i]) < e);
  }
}
