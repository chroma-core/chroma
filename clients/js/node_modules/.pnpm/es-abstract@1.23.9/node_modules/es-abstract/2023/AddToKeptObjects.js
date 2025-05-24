'use strict';

var callBound = require('call-bound');
var SLOT = require('internal-slot');

var $TypeError = require('es-errors/type');

var ClearKeptObjects = require('./ClearKeptObjects');

var isObject = require('../helpers/isObject');

var $push = callBound('Array.prototype.push');

// https://262.ecma-international.org/12.0/#sec-addtokeptobjects

module.exports = function AddToKeptObjects(object) {
	if (!isObject(object)) {
		throw new $TypeError('Assertion failed: `object` must be an Object');
	}
	$push(SLOT.get(ClearKeptObjects, '[[es-abstract internal: KeptAlive]]'), object);
};
