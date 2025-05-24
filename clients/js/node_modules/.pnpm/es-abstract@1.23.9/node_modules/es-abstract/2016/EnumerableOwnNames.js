'use strict';

var $TypeError = require('es-errors/type');

var keys = require('object-keys');

var isObject = require('../helpers/isObject');

// https://262.ecma-international.org/6.0/#sec-enumerableownnames

module.exports = function EnumerableOwnNames(O) {
	if (!isObject(O)) {
		throw new $TypeError('Assertion failed: Type(O) is not Object');
	}

	return keys(O);
};
