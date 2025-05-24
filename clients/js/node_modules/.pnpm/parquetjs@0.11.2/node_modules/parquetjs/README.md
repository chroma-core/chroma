# parquet.js

fully asynchronous, pure node.js implementation of the Parquet file format

[![Build Status](https://travis-ci.org/ironSource/parquetjs.png?branch=master)](http://travis-ci.org/ironSource/parquetjs)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![npm version](https://badge.fury.io/js/parquetjs.svg)](https://badge.fury.io/js/parquetjs)

This package contains a fully asynchronous, pure JavaScript implementation of
the [Parquet](https://parquet.apache.org/) file format. The implementation conforms with the
[Parquet specification](https://github.com/apache/parquet-format) and is tested
for compatibility with Apache's Java [reference implementation](https://github.com/apache/parquet-mr).

**What is Parquet?**: Parquet is a column-oriented file format; it allows you to
write a large amount of structured data to a file, compress it and then read parts
of it back out efficiently. The Parquet format is based on [Google's Dremel paper](https://www.google.co.nz/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0ahUKEwj_tJelpv3UAhUCm5QKHfJODhUQFggsMAE&url=http%3A%2F%2Fwww.vldb.org%2Fpvldb%2Fvldb2010%2Fpapers%2FR29.pdf&usg=AFQjCNGyMk3_JltVZjMahP6LPmqMzYdCkw).


Installation
------------

To use parquet.js with node.js, install it using npm:

```
  $ npm install parquetjs
```

_parquet.js requires node.js >= 8_


Usage: Writing files
--------------------

Once you have installed the parquet.js library, you can import it as a single
module:

``` js
var parquet = require('parquetjs');
```

Parquet files have a strict schema, similar to tables in a SQL database. So,
in order to produce a Parquet file we first need to declare a new schema. Here
is a simple example that shows how to instantiate a `ParquetSchema` object:

``` js
// declare a schema for the `fruits` table
var schema = new parquet.ParquetSchema({
  name: { type: 'UTF8' },
  quantity: { type: 'INT64' },
  price: { type: 'DOUBLE' },
  date: { type: 'TIMESTAMP_MILLIS' },
  in_stock: { type: 'BOOLEAN' }
});
```

Note that the Parquet schema supports nesting, so you can store complex, arbitrarily
nested records into a single row (more on that later) while still maintaining good
compression.

Once we have a schema, we can create a `ParquetWriter` object. The writer will
take input rows as JSON objects, convert them to the Parquet format and store
them on disk. 

``` js
// create new ParquetWriter that writes to 'fruits.parquet`
var writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');

// append a few rows to the file
await writer.appendRow({name: 'apples', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
await writer.appendRow({name: 'oranges', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
```

Once we are finished adding rows to the file, we have to tell the writer object
to flush the metadata to disk and close the file by calling the `close()` method:


Usage: Reading files
--------------------

A parquet reader allows retrieving the rows from a parquet file in order.
The basic usage is to create a reader and then retrieve a cursor/iterator
which allows you to consume row after row until all rows have been read.

You may open more than one cursor and use them concurrently. All cursors become
invalid once close() is called on
the reader object.

``` js
// create new ParquetReader that reads from 'fruits.parquet`
let reader = await parquet.ParquetReader.openFile('fruits.parquet');

// create a new cursor
let cursor = reader.getCursor();

// read all records from the file and print them
let record = null;
while (record = await cursor.next()) {
  console.log(record);
}
```

When creating a cursor, you can optionally request that only a subset of the
columns should be read from disk. For example:

``` js
// create a new cursor that will only return the `name` and `price` columns
let cursor = reader.getCursor(['name', 'price']);
```

It is important that you call close() after you are finished reading the file to
avoid leaking file descriptors.

``` js
await reader.close();
```

Encodings
---------

Internally, the Parquet format will store values from each field as consecutive
arrays which can be compressed/encoded using a number of schemes.

#### Plain Encoding (PLAIN)

The most simple encoding scheme is the PLAIN encoding. It simply stores the
values as they are without any compression. The PLAIN encoding is currently
the default for all types except `BOOLEAN`:

``` js
var schema = new parquet.ParquetSchema({
  name: { type: 'UTF8', encoding: 'PLAIN' },
});
```

#### Run Length Encoding (RLE)

The Parquet hybrid run length and bitpacking encoding allows to compress runs
of numbers very efficiently. Note that the RLE encoding can only be used in
combination with the `BOOLEAN`, `INT32` and `INT64` types. The RLE encoding
requires an additional `bitWidth` parameter that contains the maximum number of
bits required to store the largest value of the field.

``` js
var schema = new parquet.ParquetSchema({
  age: { type: 'UINT_32', encoding: 'RLE', bitWidth: 7 },
});
```


Optional Fields
---------------

By default, all fields are required to be present in each row. You can also mark
a field as 'optional' which will let you store rows with that field missing:

``` js
var schema = new parquet.ParquetSchema({
  name: { type: 'UTF8' },
  quantity: { type: 'INT64', optional: true },
});

var writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');
await writer.appendRow({name: 'apples', quantity: 10 });
await writer.appendRow({name: 'banana' }); // not in stock
```


Nested Rows & Arrays
--------------------

Parquet supports nested schemas that allow you to store rows that have a more
complex structure than a simple tuple of scalar values. To declare a schema
with a nested field, omit the `type` in the column definition and add a `fields`
list instead:

Consider this example, which allows us to store a more advanced "fruits" table
where each row contains a name, a list of colours and a list of "stock" objects. 

``` js
// advanced fruits table
var schema = new parquet.ParquetSchema({
  name: { type: 'UTF8' },
  colours: { type: 'UTF8', repeated: true },
  stock: {
    repeated: true,
    fields: {
      price: { type: 'DOUBLE' },
      quantity: { type: 'INT64' },
    }
  }
});

// the above schema allows us to store the following rows:
var writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');

await writer.appendRow({
  name: 'banana',
  colours: ['yellow'],
  stock: [
    { price: 2.45, quantity: 16 },
    { price: 2.60, quantity: 420 }
  ]
});

await writer.appendRow({
  name: 'apple',
  colours: ['red', 'green'],
  stock: [
    { price: 1.20, quantity: 42 },
    { price: 1.30, quantity: 230 }
  ]
});

await writer.close();

// reading nested rows with a list of explicit columns
let reader = await parquet.ParquetReader.openFile('fruits.parquet');

let cursor = reader.getCursor([['name'], ['stock', 'price']]);
let record = null;
while (record = await cursor.next()) {
  console.log(record);
}

await reader.close();
```

It might not be obvious why one would want to implement or use such a feature when
the same can - in  principle - be achieved by serializing the record using JSON
(or a similar scheme) and then storing it into a UTF8 field:

Putting aside the philosophical discussion on the merits of strict typing,
knowing about the structure and subtypes of all records (globally) means we do not
have to duplicate this metadata (i.e. the field names) for every record. On top
of that, knowing about the type of a field allows us to compress the remaining
data more efficiently.


List of Supported Types & Encodings
-----------------------------------

We aim to be feature-complete and add new features as they are added to the
Parquet specification; this is the list of currently implemented data types and
encodings:

<table>
  <tr><th>Logical Type</th><th>Primitive Type</th><th>Encodings</th></tr>
  <tr><td>UTF8</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>JSON</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>BSON</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>BYTE_ARRAY</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>TIME_MILLIS</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIME_MICROS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIMESTAMP_MILLIS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIMESTAMP_MICROS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>BOOLEAN</td><td>BOOLEAN</td><td>PLAIN, RLE</td></tr>
  <tr><td>FLOAT</td><td>FLOAT</td><td>PLAIN</td></tr>
  <tr><td>DOUBLE</td><td>DOUBLE</td><td>PLAIN</td></tr>
  <tr><td>INT32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT96</td><td>INT96</td><td>PLAIN</td></tr>
  <tr><td>INT_8</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_16</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>UINT_8</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>UINT_16</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>UINT_32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>UINT_64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
</table>


Buffering & Row Group Size
--------------------------

When writing a Parquet file, the `ParquetWriter` will buffer rows in memory
until a row group is complete (or `close()` is called) and then write out the row
group to disk.

The size of a row group is configurable by the user and controls the maximum
number of rows that are buffered in memory at any given time as well as the number
of rows that are co-located on disk:

``` js
var writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');
writer.setRowGroupSize(8192);
```


Depdendencies
-------------

Parquet uses [thrift](https://thrift.apache.org/) to encode the schema and other
metadata, but the actual data does not use thrift.

Contributions
-------------
Please make sure you sign the [contributor license agreement](https://github.com/ironSource/cla) in order for us to be able to accept your contribution. We thank you very much!


License
-------

Copyright (c) 2017 ironSource Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in the
Software without restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

