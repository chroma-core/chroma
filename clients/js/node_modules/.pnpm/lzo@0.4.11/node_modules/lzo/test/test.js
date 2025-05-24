'use strict';

const chai = require('chai');
const crypto = require('crypto');
const lzo = require('..');

const expect = chai.expect;
const sampleSize = parseInt(process.env.TEST_SAMPLE_SIZE, 10) || 250;

const run = plain => lzo.decompress(lzo.compress(plain), plain.length).compare(plain);
const sum = (a, b) => a + b;
const arr = len => Array(len).fill(0).map((_, i) => i);
const randChar = () => String.fromCharCode(Math.random() * 1000);
const toBuf = a => Buffer.from(a);

describe('Compression/Decompression', () => {
  it('lzo.compress should throw if nothing is passed', () =>
    expect(() => lzo.decompress()).to.throw() );

  it('lzo.decompress throw if nothing is passed', () =>
    expect(() => lzo.decompress()).to.throw() );

  it('Decompressed date should be the same as the initial input', () => {
    let random = arr(sampleSize).map(n => crypto.randomBytes(n)),
        repetetive = arr(sampleSize).map(n => randChar().repeat(n * 500)),
        result = random.concat(repetetive).map(toBuf).map(run).reduce(sum, 0);

    expect(result).to.equal(0);
  });
});

describe('Properties', () => {
  it('Should have property \'version\'', () =>
    expect(lzo).to.have.ownProperty('version') );

  it('Should have property \'versionDate\'', () =>
    expect(lzo).to.have.ownProperty('versionDate') );

  it('Should have property \'errors\' (lzo error codes)', () =>
    expect(lzo).to.have.ownProperty('errors') );
});

function compareVersion(major, minor) {
  let version = process.version.slice(1),
      parts = version.split('.').map(n => parseInt(n, 10));

  return parts[0] >= major && parts[1] >= minor;
}

if (compareVersion(10, 7)) {
  describe('Module loaded in multiple contexts (Node >= v10.7.x)', () => {
    it('Should not throw an error when module is loaded multiple times', () => {
      delete require.cache[require.resolve('..')];
      delete require.cache[require.resolve('../build/Release/node_lzo.node')];
      const lzo2 = require('..');
      expect(() => lzo2.compress(toBuf(arr(sampleSize)))).to.not.throw();
    });
  });
}
