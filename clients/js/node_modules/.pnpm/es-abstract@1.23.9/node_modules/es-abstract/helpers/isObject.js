'use strict';

module.exports = function isObject(x) {
	return !!x && (typeof x === 'function' || typeof x === 'object');
};
