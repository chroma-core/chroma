'use strict';

var $TypeError = require('es-errors/type');

var Get = require('./Get');
var IsCallable = require('./IsCallable');

var isObject = require('../helpers/isObject');

// https://262.ecma-international.org/6.0/#sec-ordinaryhasinstance

module.exports = function OrdinaryHasInstance(C, O) {
	if (!IsCallable(C)) {
		return false;
	}
	if (!isObject(O)) {
		return false;
	}
	var P = Get(C, 'prototype');
	if (!isObject(P)) {
		throw new $TypeError('OrdinaryHasInstance called on an object with an invalid prototype property.');
	}
	return O instanceof C;
};
