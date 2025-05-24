'use strict';

var $abs = require('math-intrinsics/abs');

// http://262.ecma-international.org/5.1/#sec-5.2

module.exports = function abs(x) {
	return $abs(x);
};
