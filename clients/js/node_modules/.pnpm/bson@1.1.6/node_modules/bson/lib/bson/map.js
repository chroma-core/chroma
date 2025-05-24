'use strict';

// We have an ES6 Map available, return the native instance
if (typeof global.Map !== 'undefined') {
  module.exports = global.Map;
  module.exports.Map = global.Map;
} else {
  // We will return a polyfill
  var Map = function(array) {
    this._keys = [];
    this._values = {};

    for (var i = 0; i < array.length; i++) {
      if (array[i] == null) continue; // skip null and undefined
      var entry = array[i];
      var key = entry[0];
      var value = entry[1];
      // Add the key to the list of keys in order
      this._keys.push(key);
      // Add the key and value to the values dictionary with a point
      // to the location in the ordered keys list
      this._values[key] = { v: value, i: this._keys.length - 1 };
    }
  };

  Map.prototype.clear = function() {
    this._keys = [];
    this._values = {};
  };

  Map.prototype.delete = function(key) {
    var value = this._values[key];
    if (value == null) return false;
    // Delete entry
    delete this._values[key];
    // Remove the key from the ordered keys list
    this._keys.splice(value.i, 1);
    return true;
  };

  Map.prototype.entries = function() {
    var self = this;
    var index = 0;

    return {
      next: function() {
        var key = self._keys[index++];
        return {
          value: key !== undefined ? [key, self._values[key].v] : undefined,
          done: key !== undefined ? false : true
        };
      }
    };
  };

  Map.prototype.forEach = function(callback, self) {
    self = self || this;

    for (var i = 0; i < this._keys.length; i++) {
      var key = this._keys[i];
      // Call the forEach callback
      callback.call(self, this._values[key].v, key, self);
    }
  };

  Map.prototype.get = function(key) {
    return this._values[key] ? this._values[key].v : undefined;
  };

  Map.prototype.has = function(key) {
    return this._values[key] != null;
  };

  Map.prototype.keys = function() {
    var self = this;
    var index = 0;

    return {
      next: function() {
        var key = self._keys[index++];
        return {
          value: key !== undefined ? key : undefined,
          done: key !== undefined ? false : true
        };
      }
    };
  };

  Map.prototype.set = function(key, value) {
    if (this._values[key]) {
      this._values[key].v = value;
      return this;
    }

    // Add the key to the list of keys in order
    this._keys.push(key);
    // Add the key and value to the values dictionary with a point
    // to the location in the ordered keys list
    this._values[key] = { v: value, i: this._keys.length - 1 };
    return this;
  };

  Map.prototype.values = function() {
    var self = this;
    var index = 0;

    return {
      next: function() {
        var key = self._keys[index++];
        return {
          value: key !== undefined ? self._values[key].v : undefined,
          done: key !== undefined ? false : true
        };
      }
    };
  };

  // Last ismaster
  Object.defineProperty(Map.prototype, 'size', {
    enumerable: true,
    get: function() {
      return this._keys.length;
    }
  });

  module.exports = Map;
  module.exports.Map = Map;
}
