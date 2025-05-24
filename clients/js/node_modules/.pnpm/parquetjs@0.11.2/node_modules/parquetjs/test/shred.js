'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');

describe('ParquetShredder', function() {

  it('should shred a single simple record', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 1);
    assert.deepEqual(colData.name.dlevels, [0]);
    assert.deepEqual(colData.name.rlevels, [0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple"]);
    assert.deepEqual(colData.quantity.dlevels, [0]);
    assert.deepEqual(colData.quantity.rlevels, [0]);
    assert.deepEqual(colData.quantity.values, [10]);
    assert.deepEqual(colData.price.dlevels, [0]);
    assert.deepEqual(colData.price.rlevels, [0]);
    assert.deepEqual(colData.price.values, [23.5]);
  });

  it('should shred a list of simple records', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", quantity: 20, price: 17.1 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", quantity: 15, price: 42 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.quantity.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.values, [10, 20, 15]);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
  });

  it('should shred a list of simple records with optional scalar fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', optional: true },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", price: 17.1 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", quantity: 15, price: 42 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.quantity.dlevels, [1, 0, 1]);
    assert.deepEqual(colData.quantity.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.values, [10, 15]);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
  });

  it('should shred a list of simple records with repeated scalar fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      colours: { type: 'UTF8', repeated: true },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", price: 23.5, colours: ["red", "green"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", price: 17.1, colours: ["orange"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", price: 42, colours: ["yellow"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.name.count, 3);
    assert.deepEqual(colData.colours.dlevels, [1, 1, 1, 1]);
    assert.deepEqual(colData.colours.rlevels, [0, 1, 0, 0]);
    assert.deepEqual(colData.colours.values.map((x) => x.toString()), ["red", "green", "orange", "yellow"]);
    assert.deepEqual(colData.colours.count, 4);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
    assert.deepEqual(colData.price.count, 3);
  });

  it('should shred a nested record without repetition modifiers', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        fields: {
          quantity: { type: 'INT64' },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", stock: { quantity: 10, warehouse: "A" }, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", stock: { quantity: 20, warehouse: "B" }, price: 42.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 2);
    assert.deepEqual(colData[['name']].dlevels, [0, 0]);
    assert.deepEqual(colData[['name']].rlevels, [0, 0]);
    assert.deepEqual(colData[['name']].values.map((x) => x.toString()), ["apple", "banana"]);
    assert.deepEqual(colData[['stock', 'quantity']].dlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].rlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].values, [10, 20]);
    assert.deepEqual(colData[['stock', 'warehouse']].dlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].rlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].values.map((x) => x.toString()), ["A", "B"]);
    assert.deepEqual(colData[['price']].dlevels, [0, 0]);
    assert.deepEqual(colData[['price']].rlevels, [0, 0]);
    assert.deepEqual(colData[['price']].values, [23.5, 42.0]);
  });

  it('should shred a nested record with optional fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", stock: { quantity: 10, warehouse: "A" }, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", stock: { warehouse: "B" }, price: 42.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 2);
    assert.deepEqual(colData[['name']].dlevels, [0, 0]);
    assert.deepEqual(colData[['name']].rlevels, [0, 0]);
    assert.deepEqual(colData[['name']].values.map((x) => x.toString()), ["apple", "banana"]);
    assert.deepEqual(colData[['stock', 'quantity']].dlevels, [1, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].rlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].values, [10]);
    assert.deepEqual(colData[['stock', 'warehouse']].dlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].rlevels, [0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].values.map((x) => x.toString()), ["A", "B"]);
    assert.deepEqual(colData[['price']].dlevels, [0, 0]);
    assert.deepEqual(colData[['price']].rlevels, [0, 0]);
    assert.deepEqual(colData[['price']].values, [23.5, 42.0]);
  });

  it('should shred a nested record with nested optional fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        optional: true,
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", stock: { quantity: 10, warehouse: "A" }, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange" , price: 17.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", stock: { warehouse: "B" }, price: 42.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData[['name']].dlevels, [0, 0, 0]);
    assert.deepEqual(colData[['name']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['name']].values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData[['stock', 'quantity']].dlevels, [2, 0, 1]);
    assert.deepEqual(colData[['stock', 'quantity']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].values, [10]);
    assert.deepEqual(colData[['stock', 'warehouse']].dlevels, [1, 0, 1]);
    assert.deepEqual(colData[['stock', 'warehouse']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].values.map((x) => x.toString()), ["A", "B"]);
    assert.deepEqual(colData[['price']].dlevels, [0, 0, 0]);
    assert.deepEqual(colData[['price']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['price']].values, [23.5, 17.0, 42.0]);
  });

  it('should shred a nested record with repeated fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        fields: {
          quantity: { type: 'INT64', repeated: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", stock: { quantity: 10, warehouse: "A" }, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", stock: { quantity: [50, 75], warehouse: "B" }, price: 17.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", stock: { warehouse: "C" }, price: 42.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData[['name']].dlevels, [0, 0, 0]);
    assert.deepEqual(colData[['name']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['name']].values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData[['stock', 'quantity']].dlevels, [1, 1, 1, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].rlevels, [0, 0, 1, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].values, [10, 50, 75]);
    assert.deepEqual(colData[['stock', 'warehouse']].dlevels, [0, 0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].values.map((x) => x.toString()), ["A", "B", "C"]);
    assert.deepEqual(colData[['price']].dlevels, [0, 0, 0]);
    assert.deepEqual(colData[['price']].rlevels, [0, 0, 0]);
    assert.deepEqual(colData[['price']].values, [23.5, 17.0, 42.0]);
  });

  it('should shred a nested record with nested repeated fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        repeated: true,
        fields: {
          quantity: { type: 'INT64', repeated: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", stock: [{ quantity: 10, warehouse: "A" }, { quantity: 20, warehouse: "B" } ], price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", stock: { quantity: [50, 75], warehouse: "X" }, price: 17.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "kiwi", price: 99.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", stock: { warehouse: "C" }, price: 42.0 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 4);
    assert.deepEqual(colData[['name']].dlevels, [0, 0, 0, 0]);
    assert.deepEqual(colData[['name']].rlevels, [0, 0, 0, 0]);
    assert.deepEqual(colData[['name']].values.map((x) => x.toString()), ["apple", "orange", "kiwi", "banana"]);
    assert.deepEqual(colData[['stock', 'quantity']].dlevels, [2, 2, 2, 2, 0, 1]);
    assert.deepEqual(colData[['stock', 'quantity']].rlevels, [0, 1, 0, 2, 0, 0]);
    assert.deepEqual(colData[['stock', 'quantity']].values, [10, 20, 50, 75]);
    assert.deepEqual(colData[['stock', 'warehouse']].dlevels, [1, 1, 1, 0, 1]);
    assert.deepEqual(colData[['stock', 'warehouse']].rlevels, [0, 1, 0, 0, 0]);
    assert.deepEqual(colData[['stock', 'warehouse']].values.map((x) => x.toString()), ["A", "B", "X", "C"]);
    assert.deepEqual(colData[['price']].dlevels, [0, 0, 0, 0]);
    assert.deepEqual(colData[['price']].rlevels, [0, 0, 0, 0]);
    assert.deepEqual(colData[['price']].values, [23.5, 17.0, 99.0, 42.0]);
  });

  it('should materialize a nested record with scalar repeated fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      price: { type: 'DOUBLE', repeated: true },
    });

    let buffer = {
      rowCount: 4,
      columnData: {}
    };

    buffer.columnData['name'] = {
      dlevels: [0, 0, 0, 0],
      rlevels: [0, 0, 0, 0],
      values:[
        new Buffer([97, 112, 112, 108, 101]),
        new Buffer([111, 114, 97, 110, 103, 101]),
        new Buffer([107, 105, 119, 105]),
        new Buffer([98, 97, 110, 97, 110, 97])
      ],
      count:4
    };

    buffer.columnData['price'] = {
      dlevels: [1, 1, 1, 1, 1, 1],
      rlevels: [0, 0, 1, 0, 1, 0],
      values: [23.5, 17, 23, 99, 100, 42],
      count: 6
    };

    let records = parquet.ParquetShredder.materializeRecords(schema, buffer);

    assert.equal(records.length, 4);

    assert.deepEqual(
        records[0],
        { name: "apple", price: [23.5] });

    assert.deepEqual(
        records[1],
        { name: "orange", price: [17, 23] });

    assert.deepEqual(
        records[2],
        { name: "kiwi", price: [99, 100] });

    assert.deepEqual(
        records[3],
        { name: "banana", price: [42] });
  });

  it('should materialize a nested record with nested repeated fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        repeated: true,
        fields: {
          quantity: { type: 'INT64', repeated: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });

    let buffer = {
      rowCount: 4,
      columnData: {}
    };

    buffer.columnData['name'] = {
      dlevels: [0, 0, 0, 0],
      rlevels: [0, 0, 0, 0],
      values:[
        new Buffer([97, 112, 112, 108, 101]),
        new Buffer([111, 114, 97, 110, 103, 101]),
        new Buffer([107, 105, 119, 105]),
        new Buffer([98, 97, 110, 97, 110, 97])
      ],
      count:4
    };

    buffer.columnData[['stock',  'quantity']] = {
      dlevels: [2, 2, 2, 2, 0, 1],
      rlevels: [0, 1, 0, 2, 0, 0],
      values: [10, 20, 50, 75],
      count: 6
    };

    buffer.columnData[['stock',  'warehouse']] = {
      dlevels: [1, 1, 1, 0, 1],
      rlevels: [0, 1, 0, 0, 0],
      values: [
        new Buffer([65]),
        new Buffer([66]),
        new Buffer([88]),
        new Buffer([67])
      ],
      count: 5
    };

    buffer.columnData['price'] = {
      dlevels: [0, 0, 0, 0],
      rlevels: [0, 0, 0, 0],
      values: [23.5, 17, 99, 42],
      count: 4
    };

    let records = parquet.ParquetShredder.materializeRecords(schema, buffer);

    assert.equal(records.length, 4);

    assert.deepEqual(
        records[0],
        { name: "apple", stock: [{ quantity: [10], warehouse: "A" }, { quantity: [20], warehouse: "B" } ], price: 23.5 });

    assert.deepEqual(
        records[1],
        { name: "orange", stock: [{ quantity: [50, 75], warehouse: "X" }], price: 17.0 });

    assert.deepEqual(
        records[2],
        { name: "kiwi", price: 99.0 });

    assert.deepEqual(
        records[3],
        { name: "banana", stock: [{ warehouse: "C" }], price: 42.0 });
  });

  it('should materialize a static nested record with blank optional value', function() {
    var schema = new parquet.ParquetSchema({
      fruit: {
        fields: {
          name: { type: 'UTF8' },
          colour: { type: 'UTF8', optional: true }
        }
      }
    });

    let buffer = {
      rowCount: 1,
      columnData: {}
    };

    buffer.columnData['fruit'] = {
      dlevels: [],
      rlevels: [],
      values: [],
      count: 0
    };
    
    buffer.columnData['fruit,name'] = {
      dlevels: [0],
      rlevels: [0],
      values: [
        new Buffer([97, 112, 112, 108, 101])
      ],
      count: 1
    };

    buffer.columnData['fruit,colour'] = {
      dlevels: [0],
      rlevels: [0],
      values: [],
      count: 1
    };
    
    let records = parquet.ParquetShredder.materializeRecords(schema, buffer);

    assert.equal(records.length, 1);

    assert.deepEqual(
        records[0],
        { fruit: { name: "apple" } });

  });

});
