'use strict';
const parquet = require('..');

process.on('unhandledRejection', r => console.error(r));

// write a new file 'fruits.parquet'
async function example() {
  let schema = new parquet.ParquetSchema({
    name: { type: 'UTF8' },
    price: { type: 'DOUBLE' },
    colour: { type: 'UTF8', repeated: true },
    stock: {
      repeated: true,
      fields: {
        quantity: { type: 'INT64', repeated: true },
        warehouse: { type: 'UTF8' },
      }
    },
  });

  let writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');

  await writer.appendRow({
    name: 'apples',
    price: 2.6,
    colour: [ 'green', 'red' ],
    stock: [
      { quantity: 10, warehouse: "A" },
      { quantity: 20, warehouse: "B" }
    ]
  });

  await writer.appendRow({
    name: 'oranges',
    price: 2.7,
    colour: [ 'orange' ],
    stock: {
      quantity: [50, 75],
      warehouse: "X"
    }
  });

  await writer.appendRow({
    name: 'kiwi',
    price: 4.2,
    colour: [ 'green', 'brown' ]
  });

  await writer.close();

  let reader = await parquet.ParquetReader.openFile('fruits.parquet');

  {
    let cursor = reader.getCursor();
    let record = null;
    while (record = await cursor.next()) {
      console.log(record);
    }
  }

  {
    let cursor = reader.getCursor([['name'], ['stock', 'warehouse']]);
    let record = null;
    while (record = await cursor.next()) {
      console.log(record);
    }
  }

  await reader.close();

}

example();

