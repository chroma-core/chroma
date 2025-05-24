var Module = {};
var decode = require('../decompress');
var base64 = require('base64-js');
Module['readBinary'] = function() {
  var src = base64['toByteArray'](require('../build/mem.js'));
  return decode(src);
};
