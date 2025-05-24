'use strict';
const irregularPlurals = require('./irregular-plurals.json');

module.exports = new Map(Object.entries(irregularPlurals));
