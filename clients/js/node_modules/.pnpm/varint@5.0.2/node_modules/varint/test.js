var varint = require('./index')
  , test = require('tape')
  , decode = varint.decode
  , encode = varint.encode
  , encodingLength = varint.encodingLength

test('fuzz test', function(assert) {
  var expect
    , encoded

  for(var i = 0, len = 100; i < len; ++i) {
    expect = randint(0x7FFFFFFF)
    encoded = encode(expect)
    var data = decode(encoded)
    assert.equal(expect, data, 'fuzz test: ' + expect.toString())
    assert.equal(decode.bytes, encoded.length)
  }

  assert.end()
})

test('test single byte works as expected', function(assert) {
  var buf = new Uint8Array(2)
  buf[0] = 172
  buf[1] = 2
  var data = decode(buf)
  assert.equal(data, 300, 'should equal 300')
  assert.equal(decode.bytes, 2)
  assert.end()
})

test('test encode works as expected', function(assert) {
  var out = []

  assert.deepEqual(encode(300), [0xAC, 0x02])

  assert.end()
})

test('test decode single bytes', function(assert) {
  var expected = randint(parseInt('1111111', '2'))
  var buf = new Uint8Array(1)
  buf[0] = expected
  var data = decode(buf)
  assert.equal(data, expected)
  assert.equal(decode.bytes, 1)
  assert.end()
})

test('test decode multiple bytes with zero', function(assert) {
  var expected = randint(parseInt('1111111', '2'))
  var buf = new Uint8Array(2)
  buf[0] = 128
  buf[1] = expected
  var data = decode(buf)
  assert.equal(data, expected << 7)
  assert.equal(decode.bytes, 2)
  assert.end()
})

test('encode single byte', function(assert) {
  var expected = randint(parseInt('1111111', '2'))
  assert.deepEqual(encode(expected), [expected])
  assert.equal(encode.bytes, 1)
  assert.end()
})

test('encode multiple byte with zero first byte', function(assert) {
  var expected = 0x0F00
  assert.deepEqual(encode(expected), [0x80, 0x1E])
  assert.equal(encode.bytes, 2)
  assert.end()
})

test('big integers', function (assert) {

  var bigs = []
  for(var i = 32; i <= 53; i++) (function (i) {
    bigs.push(Math.pow(2, i) - 1)
    bigs.push(Math.pow(2, i))
  })(i)

  bigs.forEach(function (n) {
    var data = encode(n)
    console.error(n, '->', data)
    assert.equal(decode(data), n)
    assert.notEqual(decode(data), n - 1)
  })
  assert.end()
})

test('fuzz test - big', function(assert) {
  var expect
    , encoded

  var MAX_INTD = Math.pow(2, 55)
  var MAX_INT = Math.pow(2, 31)

  for(var i = 0, len = 100; i < len; ++i) {
    expect = randint(MAX_INTD - MAX_INT) + MAX_INT
    encoded = encode(expect)
    var data = decode(encoded)
    assert.equal(expect, data, 'fuzz test: ' + expect.toString())
    assert.equal(decode.bytes, encoded.length)
  }

  assert.end()
})

test('encodingLength', function (assert) {

  for(var i = 0; i <= 53; i++) {
    var n = Math.pow(2, i)
    assert.equal(encode(n).length, encodingLength(n))
  }

  assert.end()
})

test('buffer too short', function (assert) {

  var value = encode(9812938912312)
  var buffer = encode(value)

  var l = buffer.length
  while(l--) {
    try {
      var val = decode(buffer.slice(0, l))
    } catch (err) {
      assert.equal(err.constructor, RangeError)
      assert.equal(decode.bytes, 0)
    }
  }
  assert.end()
})

function randint(range) {
  return Math.floor(Math.random() * range)
}
