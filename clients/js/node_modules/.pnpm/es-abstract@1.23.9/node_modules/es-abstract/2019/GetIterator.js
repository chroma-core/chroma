'use strict';

var $TypeError = require('es-errors/type');

var getIteratorMethod = require('../helpers/getIteratorMethod');
var AdvanceStringIndex = require('./AdvanceStringIndex');
var Call = require('./Call');
var GetMethod = require('./GetMethod');
var IsArray = require('./IsArray');

var isObject = require('../helpers/isObject');

var ES = {
	AdvanceStringIndex: AdvanceStringIndex,
	GetMethod: GetMethod,
	IsArray: IsArray
};

// https://262.ecma-international.org/6.0/#sec-getiterator

module.exports = function GetIterator(obj, method) {
	var actualMethod = method;
	if (arguments.length < 2) {
		actualMethod = getIteratorMethod(ES, obj);
	}
	var iterator = Call(actualMethod, obj);
	if (!isObject(iterator)) {
		throw new $TypeError('iterator must return an object');
	}

	return iterator;
};
