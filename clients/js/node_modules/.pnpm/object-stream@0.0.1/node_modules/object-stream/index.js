var stream = require('stream');

exports.fromArray = function (array) {
  var readable = new stream.Readable({ objectMode: true });
  var index = -1;
  readable._read = function () {
    if (array && index < array.length) {
      readable.push(array[++index]);
    } else {
      readable.push(null);
    }
  };
  return readable;
};

exports.map = function (iterator) {
  var transform = new stream.Transform();
  transform._readableState.objectMode = true;
  transform._writableState.objectMode = true;
  transform._transform = function (obj, encoding, next) {
    if (!iterator) return next(null, obj);
    if (iterator.length > 1) {
      iterator(obj, next);
    } else {
      next(null, iterator(obj));
    }
  };
  return transform;
};

exports.save = function (iterator, callback) {
  var writable = new stream.Writable({ objectMode: true });
  writable._write = function (obj, encoding, next) {
    try {
      if (iterator.length > 1) {
        iterator(obj, next);
      } else {
        iterator(obj);
        next();
      }
    } catch (err) {
      next(err);
    }
  };
  return writable.once('finish', function () {
    callback && callback();
  }).once('error', function (err) {
    callback && callback(err);
  });
};

exports.toArray = function (callback) {
  var writable = new stream.Writable({ objectMode: true });
  var array = [];
  writable._write = function (obj, encoding, next) {
    array.push(obj);
    next();
  };
  return writable.once('finish', function () {
    callback && callback(null, array);
  }).once('error', function (err) {
    callback && callback(err);
  });
};
