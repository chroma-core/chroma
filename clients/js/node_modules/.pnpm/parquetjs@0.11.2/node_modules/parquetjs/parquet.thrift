/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * File format description for the parquet file format
 */
namespace cpp parquet
namespace java org.apache.parquet.format

/**
 * Types supported by Parquet.  These types are intended to be used in combination
 * with the encodings to control the on disk storage format.
 * For example INT16 is not included as a type since a good encoding of INT32
 * would handle this.
 */
enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}

/**
 * Common types used by frameworks(e.g. hive, pig) using parquet.  This helps map
 * between types in those frameworks to the base types in parquet.  This is only
 * metadata and not needed to read or write the data.
 */
enum ConvertedType {
  /** a BYTE_ARRAY actually contains UTF8 encoded chars */
  UTF8 = 0;

  /** a map is converted as an optional field containing a repeated key/value pair */
  MAP = 1;

  /** a key/value pair is converted into a group of two fields */
  MAP_KEY_VALUE = 2;

  /** a list is converted into an optional field containing a repeated field for its
   * values */
  LIST = 3;

  /** an enum is converted into a binary field */
  ENUM = 4;

  /**
   * A decimal value.
   *
   * This may be used to annotate binary or fixed primitive types. The
   * underlying byte array stores the unscaled value encoded as two's
   * complement using big-endian byte order (the most significant byte is the
   * zeroth element). The value of the decimal is the value * 10^{-scale}.
   *
   * This must be accompanied by a (maximum) precision and a scale in the
   * SchemaElement. The precision specifies the number of digits in the decimal
   * and the scale stores the location of the decimal point. For example 1.23
   * would have precision 3 (3 total digits) and scale 2 (the decimal point is
   * 2 digits over).
   */
  DECIMAL = 5;

  /**
   * A Date
   *
   * Stored as days since Unix epoch, encoded as the INT32 physical type.
   *
   */
  DATE = 6; 

  /** 
   * A time 
   *
   * The total number of milliseconds since midnight.  The value is stored 
   * as an INT32 physical type.
   */
  TIME_MILLIS = 7;

  /**
   * A time.
   *
   * The total number of microseconds since midnight.  The value is stored as
   * an INT64 physical type.
   */
  TIME_MICROS = 8;

  /**
   * A date/time combination
   * 
   * Date and time recorded as milliseconds since the Unix epoch.  Recorded as
   * a physical type of INT64.
   */
  TIMESTAMP_MILLIS = 9; 

  /**
   * A date/time combination
   *
   * Date and time recorded as microseconds since the Unix epoch.  The value is
   * stored as an INT64 physical type.
   */
  TIMESTAMP_MICROS = 10;


  /** 
   * An unsigned integer value.  
   * 
   * The number describes the maximum number of meainful data bits in 
   * the stored value. 8, 16 and 32 bit values are stored using the 
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   *
   */
  UINT_8 = 11;
  UINT_16 = 12;
  UINT_32 = 13;
  UINT_64 = 14;

  /**
   * A signed integer value.
   *
   * The number describes the maximum number of meainful data bits in
   * the stored value. 8, 16 and 32 bit values are stored using the
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   *
   */
  INT_8 = 15;
  INT_16 = 16;
  INT_32 = 17;
  INT_64 = 18;

  /** 
   * An embedded JSON document
   * 
   * A JSON document embedded within a single UTF8 column.
   */
  JSON = 19;

  /** 
   * An embedded BSON document
   * 
   * A BSON document embedded within a single BINARY column. 
   */
  BSON = 20;

  /**
   * An interval of time
   * 
   * This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
   * This data is composed of three separate little endian unsigned
   * integers.  Each stores a component of a duration of time.  The first
   * integer identifies the number of months associated with the duration,
   * the second identifies the number of days associated with the duration
   * and the third identifies the number of milliseconds associated with 
   * the provided duration.  This duration of time is independent of any
   * particular timezone or date.
   */
  INTERVAL = 21;

  /**
   * Annotates a column that is always null
   * Sometimes when discovering the schema of existing data
   * values are always null
   */
  NULL = 25;
}

/**
 * Representation of Schemas
 */
enum FieldRepetitionType {
  /** This field is required (can not be null) and each record has exactly 1 value. */
  REQUIRED = 0;

  /** The field is optional (can be null) and each record has 0 or 1 values. */
  OPTIONAL = 1;

  /** The field is repeated and can contain 0 or more values */
  REPEATED = 2;
}

/**
 * Statistics per row group and per page
 * All fields are optional.
 */
struct Statistics {
   /** min and max value of the column, encoded in PLAIN encoding */
   1: optional binary max;
   2: optional binary min;
   /** count of null value in the column */
   3: optional i64 null_count;
   /** count of distinct values occurring */
   4: optional i64 distinct_count;
}

/**
 * Represents a element inside a schema definition.
 *  - if it is a group (inner node) then type is undefined and num_children is defined
 *  - if it is a primitive type (leaf) then type is defined and num_children is undefined
 * the nodes are listed in depth first traversal order.
 */
struct SchemaElement {
  /** Data type for this field. Not set if the current element is a non-leaf node */
  1: optional Type type;

  /** If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the vales.
   * Otherwise, if specified, this is the maximum bit length to store any of the values.
   * (e.g. a low cardinality INT col could have this set to 3).  Note that this is
   * in the schema, and therefore fixed for the entire file.
   */
  2: optional i32 type_length;

  /** repetition of the field. The root of the schema does not have a repetition_type.
   * All other nodes must have one */
  3: optional FieldRepetitionType repetition_type;

  /** Name of the field in the schema */
  4: required string name;

  /** Nested fields.  Since thrift does not support nested fields,
   * the nesting is flattened to a single list by a depth-first traversal.
   * The children count is used to construct the nested relationship.
   * This field is not set when the element is a primitive type
   */
  5: optional i32 num_children;

  /** When the schema is the result of a conversion from another model
   * Used to record the original type to help with cross conversion.
   */
  6: optional ConvertedType converted_type;

  /** Used when this column contains decimal data.
   * See the DECIMAL converted type for more details.
   */
  7: optional i32 scale
  8: optional i32 precision

  /** When the original schema supports field ids, this will save the
   * original field id in the parquet schema
   */
  9: optional i32 field_id;

}

/**
 * Encodings supported by Parquet.  Not all encodings are valid for all types.  These
 * enums are also used to specify the encoding of definition and repetition levels.
 * See the accompanying doc for the details of the more complicated encodings.
 */
enum Encoding {
  /** Default encoding.
   * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
   * INT32 - 4 bytes per value.  Stored as little-endian.
   * INT64 - 8 bytes per value.  Stored as little-endian.
   * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
   * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
   * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
   * FIXED_LEN_BYTE_ARRAY - Just the bytes.
   */
  PLAIN = 0;

  /** Group VarInt encoding for INT32/INT64.
   * This encoding is deprecated. It was never used
   */
  //  GROUP_VAR_INT = 1;

  /**
   * Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
   * plain type.
   * in a data page use RLE_DICTIONARY instead.
   * in a Dictionary page use PLAIN instead
   */
  PLAIN_DICTIONARY = 2;

  /** Group packed run length encoding. Usable for definition/reptition levels
   * encoding and Booleans (on one bit: 0 is false; 1 is true.)
   */
  RLE = 3;

  /** Bit packed encoding.  This can only be used if the data has a known max
   * width.  Usable for definition/repetition levels encoding.
   */
  BIT_PACKED = 4;

  /** Delta encoding for integers. This can be used for int columns and works best
   * on sorted data
   */
  DELTA_BINARY_PACKED = 5;

  /** Encoding for byte arrays to separate the length values and the data. The lengths
   * are encoded using DELTA_BINARY_PACKED
   */
  DELTA_LENGTH_BYTE_ARRAY = 6;

  /** Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
   * Suffixes are stored as delta length byte arrays.
   */
  DELTA_BYTE_ARRAY = 7;

  /** Dictionary encoding: the ids are encoded using the RLE encoding
   */
  RLE_DICTIONARY = 8;
}

/**
 * Supported compression algorithms.
 */
enum CompressionCodec {
  UNCOMPRESSED = 0;
  SNAPPY = 1;
  GZIP = 2;
  LZO = 3;
  BROTLI = 4;
}

enum PageType {
  DATA_PAGE = 0;
  INDEX_PAGE = 1;
  DICTIONARY_PAGE = 2;
  DATA_PAGE_V2 = 3;
}

/** Data page header */
struct DataPageHeader {
  /** Number of values, including NULLs, in this data page. **/
  1: required i32 num_values

  /** Encoding used for this data page **/
  2: required Encoding encoding

  /** Encoding used for definition levels **/
  3: required Encoding definition_level_encoding;

  /** Encoding used for repetition levels **/
  4: required Encoding repetition_level_encoding;

  /** Optional statistics for the data in this page**/
  5: optional Statistics statistics;
}

struct IndexPageHeader {
  /** TODO: **/
}

struct DictionaryPageHeader {
  /** Number of values in the dictionary **/
  1: required i32 num_values;

  /** Encoding using this dictionary page **/
  2: required Encoding encoding

  /** If true, the entries in the dictionary are sorted in ascending order **/
  3: optional bool is_sorted;
}

/**
 * New page format alowing reading levels without decompressing the data
 * Repetition and definition levels are uncompressed
 * The remaining section containing the data is compressed if is_compressed is true
 **/
struct DataPageHeaderV2 {
  /** Number of values, including NULLs, in this data page. **/
  1: required i32 num_values
  /** Number of NULL values, in this data page.
      Number of non-null = num_values - num_nulls which is also the number of values in the data section **/
  2: required i32 num_nulls
  /** Number of rows in this data page. which means pages change on record boundaries (r = 0) **/
  3: required i32 num_rows
  /** Encoding used for data in this page **/
  4: required Encoding encoding

  // repetition levels and definition levels are always using RLE (without size in it)

  /** length of the repetition levels */
  5: required i32 definition_levels_byte_length;
  /** length of the definition levels */
  6: required i32 repetition_levels_byte_length;

  /**  whether the values are compressed.
  Which means the section of the page between
  definition_levels_byte_length + repetition_levels_byte_length + 1 and compressed_page_size (included)
  is compressed with the compression_codec.
  If missing it is considered compressed */
  7: optional bool is_compressed = 1;

  /** optional statistics for this column chunk */
  8: optional Statistics statistics;
}

struct PageHeader {
  /** the type of the page: indicates which of the *_header fields is set **/
  1: required PageType type

  /** Uncompressed page size in bytes (not including this header) **/
  2: required i32 uncompressed_page_size

  /** Compressed page size in bytes (not including this header) **/
  3: required i32 compressed_page_size

  /** 32bit crc for the data below. This allows for disabling checksumming in HDFS
   *  if only a few pages needs to be read
   **/
  4: optional i32 crc

  // Headers for page specific data.  One only will be set.
  5: optional DataPageHeader data_page_header;
  6: optional IndexPageHeader index_page_header;
  7: optional DictionaryPageHeader dictionary_page_header;
  8: optional DataPageHeaderV2 data_page_header_v2;
}

/**
 * Wrapper struct to store key values
 */
 struct KeyValue {
  1: required string key
  2: optional string value
}

/**
 * Wrapper struct to specify sort order
 */
struct SortingColumn {
  /** The column index (in this row group) **/
  1: required i32 column_idx

  /** If true, indicates this column is sorted in descending order. **/
  2: required bool descending

  /** If true, nulls will come before non-null values, otherwise,
   * nulls go at the end. */
  3: required bool nulls_first
}

/**
 * statistics of a given page type and encoding
 */
struct PageEncodingStats {

  /** the page type (data/dic/...) **/
  1: required PageType page_type;

  /** encoding of the page **/
  2: required Encoding encoding;

  /** number of pages of this type with this encoding **/
  3: required i32 count;

}

/**
 * Description for column metadata
 */
struct ColumnMetaData {
  /** Type of this column **/
  1: required Type type

  /** Set of all encodings used for this column. The purpose is to validate
   * whether we can decode those pages. **/
  2: required list<Encoding> encodings

  /** Path in schema **/
  3: required list<string> path_in_schema

  /** Compression codec **/
  4: required CompressionCodec codec

  /** Number of values in this column **/
  5: required i64 num_values

  /** total byte size of all uncompressed pages in this column chunk (including the headers) **/
  6: required i64 total_uncompressed_size

  /** total byte size of all compressed pages in this column chunk (including the headers) **/
  7: required i64 total_compressed_size

  /** Optional key/value metadata **/
  8: optional list<KeyValue> key_value_metadata

  /** Byte offset from beginning of file to first data page **/
  9: required i64 data_page_offset

  /** Byte offset from beginning of file to root index page **/
  10: optional i64 index_page_offset

  /** Byte offset from the beginning of file to first (only) dictionary page **/
  11: optional i64 dictionary_page_offset

  /** optional statistics for this column chunk */
  12: optional Statistics statistics;

  /** Set of all encodings used for pages in this column chunk.
   * This information can be used to determine if all data pages are
   * dictionary encoded for example **/
  13: optional list<PageEncodingStats> encoding_stats;
}

struct ColumnChunk {
  /** File where column data is stored.  If not set, assumed to be same file as
    * metadata.  This path is relative to the current file.
    **/
  1: optional string file_path

  /** Byte offset in file_path to the ColumnMetaData **/
  2: required i64 file_offset

  /** Column metadata for this chunk. This is the same content as what is at
   * file_path/file_offset.  Having it here has it replicated in the file
   * metadata.
   **/
  3: optional ColumnMetaData meta_data
}

struct RowGroup {
  /** Metadata for each column chunk in this row group.
   * This list must have the same order as the SchemaElement list in FileMetaData.
   **/
  1: required list<ColumnChunk> columns

  /** Total byte size of all the uncompressed column data in this row group **/
  2: required i64 total_byte_size

  /** Number of rows in this row group **/
  3: required i64 num_rows

  /** If set, specifies a sort ordering of the rows in this RowGroup.
   * The sorting columns can be a subset of all the columns.
   */
  4: optional list<SortingColumn> sorting_columns
}

/**
 * Description for file metadata
 */
struct FileMetaData {
  /** Version of this file **/
  1: required i32 version

  /** Parquet schema for this file.  This schema contains metadata for all the columns.
   * The schema is represented as a tree with a single root.  The nodes of the tree
   * are flattened to a list by doing a depth-first traversal.
   * The column metadata contains the path in the schema for that column which can be
   * used to map columns to nodes in the schema.
   * The first element is the root **/
  2: required list<SchemaElement> schema;

  /** Number of rows in this file **/
  3: required i64 num_rows

  /** Row groups in this file **/
  4: required list<RowGroup> row_groups

  /** Optional key/value metadata **/
  5: optional list<KeyValue> key_value_metadata

  /** String for application that wrote this file.  This should be in the format
   * <Application> version <App Version> (build <App Build Hash>).
   * e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
   **/
  6: optional string created_by
}

