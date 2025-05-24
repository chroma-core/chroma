# object-stream

Simplified object streams based on Node streams2.

## Installation

```bash
npm install object-stream
```

## Examples

#### Creating a readable stream of objects from an array

```js
var objectStream = require('object-stream');
objectStream.fromArray([1, 2, 3]).pipe(...);
```

Pass an empty array or anything falsy to create an empty stream.

#### Collecting elements into an array

```js
var objectStream = require('object-stream');
objectStream.fromArray([1, 2, 3])
  .pipe(objectStream.toArray(function (err, array) {
    /* array contains all items emitted from readable streams */
  }));
```

#### Mapping objects through a transform stream

Synchronously (iterator has only one argument):

```js
var objectStream = require('object-stream');

objectStream.fromArray([1, 2, 3])
  .pipe(objectStream.map(function (value) {
    return value * 2;
  }))
  .pipe(...);

// asynchronous
objectStream.fromArray([1, 2, 3])
  .pipe(objectStream.map(function (value, callback) {
    setTimeout(function () {
      callback(null, value * 2);
    }, 100);
  }))
  .pipe(...);
```

Asynchronously (iterator has two arguments: value and callback):

```js
var objectStream = require('object-stream');

objectStream.fromArray([1, 2, 3])
  .pipe(objectStream.map(function (value, callback) {
    setTimeout(function () {
      callback(null, value * 2);
    }, 100);
  }))
  .pipe(...);
```

#### Saving objects through a writable stream (or any other write-like action)

Synchronously (iterator has only one argument):

```js
var objectStream = require('object-stream');

function save(value) {
  /* save item... */
};

objectStream.fromArray([1, 2, 3]).pipe(objectStream.save(save));
```

Asynchronously (iterator has two arguments: value and callback):

```js
var objectStream = require('object-stream');

function saveAsync(value, callback) {
  /* save item... */
  callback();
};

objectStream.fromArray([1, 2, 3]).pipe(objectStream.save(saveAsync));
```

## License

The MIT License (MIT)

Copyright (c) 2014, Nicolas Mercier

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
