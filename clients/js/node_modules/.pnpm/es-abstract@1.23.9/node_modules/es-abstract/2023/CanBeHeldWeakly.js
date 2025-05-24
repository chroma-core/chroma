'use strict';

var KeyForSymbol = require('./KeyForSymbol');

var isObject = require('../helpers/isObject');

// https://262.ecma-international.org/14.0/#sec-canbeheldweakly

module.exports = function CanBeHeldWeakly(v) {
	if (isObject(v)) {
		return true; // step 1
	}
	if (typeof v === 'symbol' && typeof KeyForSymbol(v) === 'undefined') {
		return true; // step 2
	}
	return false; // step 3
};
