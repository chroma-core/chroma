'use strict';

var define = require('define-properties');
var RequireObjectCoercible = require('es-object-atoms/RequireObjectCoercible');
var callBind = require('call-bind');

var implementation = require('./implementation');
var getPolyfill = require('./polyfill');
var shim = require('./shim');

var bound = callBind.apply(getPolyfill());

var boundPadEnd = function padEnd(str, maxLength) {
	RequireObjectCoercible(str);
	var args = arguments.length > 2 ? [maxLength, arguments[2]] : [maxLength];
	return bound(str, args);
};

define(boundPadEnd, {
	getPolyfill: getPolyfill,
	implementation: implementation,
	shim: shim
});

module.exports = boundPadEnd;
