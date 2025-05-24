require('should');
var os = require('../');
var stream = require('stream');

describe('objectStream.fromArray(array)', function () {
  it('should return a readable stream', function () {
    os.fromArray().should.be.an.instanceOf(stream.Readable);
  });
  it('should should stream array elements', function (done) {
    os.fromArray([1, 2, 3]).pipe(os.toArray(function (err, array) {
      array.should.eql([1, 2, 3]);
      done();
    }));
  });
});

describe('objectStream.map(iterator)', function () {
  it('should return a transform stream', function () {
    os.map().should.be.an.instanceOf(stream.Transform);
  });
  it('should should map elements synchronously', function (done) {
    os.fromArray([1, 2, 3]).pipe(os.map(function (value) {
      return value * 2;
    })).pipe(os.toArray(function (err, array) {
      array.should.eql([2, 4, 6]);
      done();
    }));
  });
  it('should should map elements asynchronously', function (done) {
    os.fromArray([1, 2, 3]).pipe(os.map(function (value, callback) {
      setImmediate(function () {
        callback(null, value * 3);
      });
    })).pipe(os.toArray(function (err, array) {
      array.should.eql([3, 6, 9]);
      done();
    }));
  });
});

describe('objectStream.save(iterator, callback)', function () {
  it('should return a writable stream', function () {
    os.save().should.be.an.instanceOf(stream.Writable);
  });
  it('should should save elements synchronously', function () {
    var count = 0;
    var save = function () { count++; };
    os.fromArray([1, 2, 3]).pipe(os.save(save, function (err) {
      count.should.be.exactly(3);
    }));
  });
  it('should should save elements asynchronously', function (done) {
    var count = 0;
    var save = function (value, callback) {
      setImmediate(function () {
        count++;
        callback();
      });
    };
    os.fromArray([1, 2, 3]).pipe(os.save(save, function (err) {
      count.should.be.exactly(3);
      done();
    }));
  });
  it('should handle errors thrown synchronously', function () {
    os.fromArray([1, 2, 3]).pipe(os.save(function () {
      throw new Error('failure');
    }, function (err) {
      err.should.be.an.Error.and.have.property('message', 'failure');
    }));
  });
  it('should handle errors raised asynchronously', function (done) {
    os.fromArray([1, 2, 3]).pipe(os.save(function (value, callback) {
      return callback(new Error('failure'));
    }, function (err) {
      err.should.be.an.Error.and.have.property('message', 'failure');
      done();
    }));
  });
});

describe('objectStream.toArray(callback)', function () {
  it('should return a writable stream', function () {
    os.toArray().should.be.an.instanceOf(stream.Writable);
  });
  it('should should concatenate array elements', function (done) {
    os.fromArray([1, 2, 3]).pipe(os.toArray(function (err, array) {
      array.should.eql([1, 2, 3]);
      done();
    }));
  });
});
