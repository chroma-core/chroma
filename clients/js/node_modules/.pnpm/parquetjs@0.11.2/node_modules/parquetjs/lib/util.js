'use strict';
const fs = require('fs');
const thrift = require('thrift');

/**
 * Helper function that serializes a thrift object into a buffer
 */
exports.serializeThrift = function(obj) {
  let output = []

  let transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })

  let protocol = new thrift.TCompactProtocol(transport)
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
}

exports.decodeThrift = function(obj, buf, offset) {
  if (!offset) {
    offset = 0;
  }

  var transport = new thrift.TFramedTransport(buf);
  transport.readPos = offset;
  var protocol = new thrift.TCompactProtocol(transport);
  obj.read(protocol);
  return transport.readPos - offset;
}

/**
 * Get the number of bits required to store a given value
 */
exports.getBitWidth = function(val) {
  if (val === 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * FIXME not ideal that this is linear
 */
exports.getThriftEnum = function(klass, value) {
  for (let k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }

  throw 'Invalid ENUM value';
}

exports.fopen = function(filePath) {
  return new Promise((resolve, reject) => {
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    })
  });
}

exports.fstat = function(filePath) {
  return new Promise((resolve, reject) => {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        reject(err);
      } else {
        resolve(stat);
      }
    })
  });
}

exports.fread = function(fd, position, length) {
  let buffer = Buffer.alloc(length);

  return new Promise((resolve, reject) => {
    fs.read(fd, buffer, 0, length, position, (err, bytesRead, buf) => {
      if (err || bytesRead != length) {
        reject(err || Error('read failed'));
      } else {
        resolve(buf);
      }
    });
  });
}

exports.fclose = function(fd) {
  return new Promise((resolve, reject) => {
    fs.close(fd, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
}

exports.oswrite = function(os, buf) {
  return new Promise((resolve, reject) => {
    os.write(buf, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

exports.osclose = function(os) {
  return new Promise((resolve, reject) => {
    os.close((err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

exports.osopen = function(path, opts) {
  return new Promise((resolve, reject) => {
    let outputStream = fs.createWriteStream(path, opts);

    outputStream.on('open', function(fd) {
      resolve(outputStream);
    });

    outputStream.on('error', function(err) {
      reject(err);
    });
  });
}

exports.fieldIndexOf = function(arr, elem) {
  for (let j = 0; j < arr.length; ++j) {
    if (arr[j].length !== elem.length) {
      continue;
    }

    let m = true;
    for (let i = 0; i < elem.length; ++i) {
      if (arr[j][i] !== elem[i]) {
        m = false;
        break;
      }
    }

    if (m) {
      return j;
    }
  }

  return -1;
}

