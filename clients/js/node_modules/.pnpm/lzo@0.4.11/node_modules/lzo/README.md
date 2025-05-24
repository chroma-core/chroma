# node-lzo [![npm version](https://badge.fury.io/js/lzo.svg)](https://badge.fury.io/js/lzo)
Node.js Bindings for [LZO Compression](http://www.oberhumer.com/opensource/lzo/)

## Example
```javascript
const lzo = require('lzo');

console.log('Current version:', lzo.version, '-', lzo.versionDate);

let str = 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam',
    compressed = lzo.compress(str);

console.log('Original Length:', str.length, '-- Compressed length:', compressed.length);

let decompressed = lzo.decompress(compressed);

console.log('Decompressed Length:', decompressed.length);
console.log(decompressed.toString());
```

## Properties
#### version
The version of [LZO](http://www.oberhumer.com/opensource/lzo/) being used.

#### versionDate
The date on which the version was released.

#### errors
An object containing the lzo error codes as seen below.


## Methods
#### compress(data, *length*)
If *data* is not a Buffer, the function will try to convert it via `Buffer.from`.
If you specify a *length*, the function will allocate that much memory for the compressed data.  
Returns the compressed data as a Buffer.

#### decompress(data, *length*)
If *data* is not a Buffer, the function will try to convert it via `Buffer.from`.
If you specify a *length*, the function will allocate that much memory for the decompressed data. I suggest you to do so whenever you know the length.  
Returns the decompressed data as a Buffer.

## Errors
Code | Description
-------------: | :------------- 
`-1` | LZO\_E\_ERROR
`-2` | LZO\_E\_OUT\_OF\_MEMORY
`-3` | LZO\_E\_NOT\_COMPRESSIBLE
`-4` | LZO\_E\_INPUT\_OVERRUN
`-5` | LZO\_E\_OUTPUT\_OVERRUN
`-6` | LZO\_E\_LOOKBEHIND_OVERRUN
`-7` | LZO\_E\_EOF\_NOT\_FOUND
`-8` | LZO\_E\_INPUT\_NOT\_CONSUMED
`-9` | LZO\_E\_NOT\_YET\_IMPLEMENTED
`-10` | LZO\_E\_INVALID\_ARGUMENT
`-11` | LZO\_E\_INVALID\_ALIGNMENT
`-12` | LZO\_E\_OUTPUT\_NOT\_CONSUMED
`-99` | LZO\_E\_INTERNAL\_ERROR
