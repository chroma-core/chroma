'use strict';
const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_shredder = require('./shred')
const parquet_util = require('./util')
const parquet_schema = require('./schema')
const parquet_codec = require('./codec')
const parquet_compression = require('./compression')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Internal type used for repetition/definition levels
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
class ParquetCursor  {

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(metadata, envelopeReader, schema, columnList) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  async next() {
    if (this.rowGroup.length === 0) {
      if (this.rowGroupIndex >= this.metadata.row_groups.length) {
        return null;
      }

      let rowBuffer = await this.envelopeReader.readRowGroup(
          this.schema,
          this.metadata.row_groups[this.rowGroupIndex],
          this.columnList);

      this.rowGroup = parquet_shredder.materializeRecords(this.schema, rowBuffer);
      this.rowGroupIndex++;
    }

    return this.rowGroup.shift();
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind() {
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

};

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
class ParquetReader {

  /**
   * Open the parquet file pointed to by the specified path and return a new
   * parquet reader
   */
  static async openFile(filePath) {
    let envelopeReader = await ParquetEnvelopeReader.openFile(filePath);

    try {
      await envelopeReader.readHeader();
      let metadata = await envelopeReader.readFooter();
      return new ParquetReader(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is not recommended to call this constructor directly except for advanced
   * and internal use cases. Consider using one of the open{File,Buffer} methods
   * instead
   */
  constructor(metadata, envelopeReader) {
    if (metadata.version != PARQUET_VERSION) {
      throw 'invalid parquet version';
    }

    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = new parquet_schema.ParquetSchema(
        decodeSchema(
            this.metadata.schema.splice(1)));
  }

  /**
   * Return a cursor to the file. You may open more than one cursor and use
   * them concurrently. All cursors become invalid once close() is called on
   * the reader object.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   */
  getCursor(columnList) {
    if (!columnList) {
      columnList = [];
    }

    columnList = columnList.map((x) => x.constructor === Array ? x : [x]);

    return new ParquetCursor(
        this.metadata, 
        this.envelopeReader,
        this.schema,
        columnList);
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not neccessarily equal to the number of rows in each column.
   */
  getRowCount() {
    return this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema() {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata() {
    let md = {};
    for (let kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }

    return md;
  }

  /**
   * Close this parquet reader. You MUST call this method once you're finished
   * reading rows
   */
  async close() {
    await this.envelopeReader.close();
    this.envelopeReader = null;
    this.metadata = null;
  }

}

/**
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
 */
class ParquetEnvelopeReader {

  static async openFile(filePath) {
    let fileStat = await parquet_util.fstat(filePath);
    let fileDescriptor = await parquet_util.fopen(filePath);

    let readFn = parquet_util.fread.bind(undefined, fileDescriptor);
    let closeFn = parquet_util.fclose.bind(undefined, fileDescriptor);

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size);
  }

  constructor(readFn, closeFn, fileSize) {
    this.read = readFn;
    this.close = closeFn;
    this.fileSize = fileSize;
  }

  async readHeader() {
    let buf = await this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() != PARQUET_MAGIC) {
      throw 'not valid parquet file'
    }
  }

  async readRowGroup(schema, rowGroup, columnList) {
    var buffer = {
      rowCount: +rowGroup.num_rows,
      columnData: {}
    };

    for (let colChunk of rowGroup.columns) {
      const colMetadata = colChunk.meta_data;
      const colKey = colMetadata.path_in_schema;

      if (columnList.length > 0 && parquet_util.fieldIndexOf(columnList, colKey) < 0) {
        continue;
      }

      buffer.columnData[colKey] = await this.readColumnChunk(schema, colChunk);
    }

    return buffer;
  }

  async readColumnChunk(schema, colChunk) {
    if (colChunk.file_path !== null) {
      throw 'external references are not supported';
    }

    let field = schema.findField(colChunk.meta_data.path_in_schema);
    let type = parquet_util.getThriftEnum(
        parquet_thrift.Type,
        colChunk.meta_data.type);

    let compression = parquet_util.getThriftEnum(
        parquet_thrift.CompressionCodec,
        colChunk.meta_data.codec);

    let pagesOffset = +colChunk.meta_data.data_page_offset;
    let pagesSize = +colChunk.meta_data.total_compressed_size;
    let pagesBuf = await this.read(pagesOffset, pagesSize);

    return decodeDataPages(pagesBuf, {
      type: type,
      rLevelMax: field.rLevelMax,
      dLevelMax: field.dLevelMax,
      compression: compression,
      column: field
    });
  }

  async readFooter() {
    let trailerLen = PARQUET_MAGIC.length + 4;
    let trailerBuf = await this.read(this.fileSize - trailerLen, trailerLen);

    if (trailerBuf.slice(4).toString() != PARQUET_MAGIC) {
      throw 'not a valid parquet file';
    }

    let metadataSize = trailerBuf.readUInt32LE(0);
    let metadataOffset = this.fileSize - metadataSize - trailerLen;
    if (metadataOffset < PARQUET_MAGIC.length) {
      throw 'invalid metadata size';
    }

    let metadataBuf = await this.read(metadataOffset, metadataSize);
    let metadata = new parquet_thrift.FileMetaData();
    parquet_util.decodeThrift(metadata, metadataBuf);
    return metadata;
  }

}

/**
 * Decode a consecutive array of data using one of the parquet encodings
 */
function decodeValues(type, encoding, cursor, count, opts) {
  if (!(encoding in parquet_codec)) {
    throw 'invalid encoding: ' + encoding;
  }

  return parquet_codec[encoding].decodeValues(type, cursor, count, opts);
}

function decodeDataPages(buffer, opts) {
  let cursor = {
    buffer: buffer,
    offset: 0,
    size: buffer.length
  };

  let data = {
    rlevels: [],
    dlevels: [],
    values: [],
    count: 0
  };

  while (cursor.offset < cursor.size) {
    const pageHeader = new parquet_thrift.PageHeader();
    cursor.offset += parquet_util.decodeThrift(pageHeader, cursor.buffer);

    const pageType = parquet_util.getThriftEnum(
        parquet_thrift.PageType,
        pageHeader.type);

    let pageData = null;
    switch (pageType) {
      case 'DATA_PAGE':
        pageData = decodeDataPage(cursor, pageHeader, opts);
        break;
      case 'DATA_PAGE_V2':
        pageData = decodeDataPageV2(cursor, pageHeader, opts);
        break;
      default:
        throw "invalid page type: " + pageType;
    }

    Array.prototype.push.apply(data.rlevels, pageData.rlevels);
    Array.prototype.push.apply(data.dlevels, pageData.dlevels);
    Array.prototype.push.apply(data.values, pageData.values);
    data.count += pageData.count;
  }


  return data;
}

function decodeDataPage(cursor, header, opts) {
  let valueCount = header.data_page_header.num_values;
  let valueEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.encoding);

  /* read repetition levels */
  let rLevelEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.repetition_level_encoding);

  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        rLevelEncoding,
        cursor,
        valueCount,
        { bitWidth: parquet_util.getBitWidth(opts.rLevelMax) });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevelEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.definition_level_encoding);

  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        dLevelEncoding,
        cursor,
        valueCount,
        { bitWidth: parquet_util.getBitWidth(opts.dLevelMax) });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valueCountNonNull = 0;
  for (let dlvl of dLevels) {
    if (dlvl === opts.dLevelMax) {
      ++valueCountNonNull;
    }
  }

  let values = decodeValues(
      opts.type,
      valueEncoding,
      cursor,
      valueCountNonNull,
      {
        typeLength: opts.column.typeLength,
        bitWidth: opts.column.typeLength
      });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeDataPageV2(cursor, header, opts) {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  const valueCount = header.data_page_header_v2.num_values;
  const valueCountNonNull = valueCount - header.data_page_header_v2.num_nulls;
  const valueEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header_v2.encoding);

  /* read repetition levels */
  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        PARQUET_RDLVL_ENCODING,
        cursor,
        valueCount,
        {
          bitWidth: parquet_util.getBitWidth(opts.rLevelMax),
          disableEnvelope: true
        });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        PARQUET_RDLVL_ENCODING,
        cursor,
        valueCount,
        {
          bitWidth: parquet_util.getBitWidth(opts.dLevelMax),
          disableEnvelope: true
        });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valuesBufCursor = cursor;

  if (header.data_page_header_v2.is_compressed) {
    let valuesBuf = parquet_compression.inflate(
        opts.compression,
        cursor.buffer.slice(cursor.offset, cursorEnd));

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };

    cursor.offset = cursorEnd;
  }

  let values = decodeValues(
      opts.type,
      valueEncoding,
      valuesBufCursor,
      valueCountNonNull,
      {
        typeLength: opts.column.typeLength,
        bitWidth: opts.column.typeLength
      });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeSchema(schemaElements) {
  let schema = {};
  for (let idx = 0; idx < schemaElements.length; ) {
    const schemaElement = schemaElements[idx];

    let repetitionType = parquet_util.getThriftEnum(
        parquet_thrift.FieldRepetitionType,
        schemaElement.repetition_type);

    let optional = false;
    let repeated = false;
    switch (repetitionType) {
      case 'REQUIRED':
        break;
      case 'OPTIONAL':
        optional = true;
        break;
      case 'REPEATED':
        repeated = true;
        break;
    };

    if (schemaElement.num_children > 0) {
      schema[schemaElement.name] = {
        optional: optional,
        repeated: repeated,
        fields: decodeSchema(
            schemaElements.slice(idx + 1, idx + 1 + schemaElement.num_children))
      };
    } else {
      let logicalType = parquet_util.getThriftEnum(
          parquet_thrift.Type,
          schemaElement.type);

      if (schemaElement.converted_type != null) {
        logicalType = parquet_util.getThriftEnum(
            parquet_thrift.ConvertedType,
            schemaElement.converted_type);
      }

      schema[schemaElement.name] = {
        type: logicalType,
        typeLength: schemaElement.type_length,
        optional: optional,
        repeated: repeated
      };
    }

    idx += (schemaElement.num_children || 0) + 1;
  }

  return schema;
}

module.exports = {
  ParquetEnvelopeReader,
  ParquetReader,
};

