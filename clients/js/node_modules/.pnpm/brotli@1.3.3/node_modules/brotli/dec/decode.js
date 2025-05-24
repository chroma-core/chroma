/* Copyright 2013 Google Inc. All Rights Reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

var BrotliInput = require('./streams').BrotliInput;
var BrotliOutput = require('./streams').BrotliOutput;
var BrotliBitReader = require('./bit_reader');
var BrotliDictionary = require('./dictionary');
var HuffmanCode = require('./huffman').HuffmanCode;
var BrotliBuildHuffmanTable = require('./huffman').BrotliBuildHuffmanTable;
var Context = require('./context');
var Prefix = require('./prefix');
var Transform = require('./transform');

var kDefaultCodeLength = 8;
var kCodeLengthRepeatCode = 16;
var kNumLiteralCodes = 256;
var kNumInsertAndCopyCodes = 704;
var kNumBlockLengthCodes = 26;
var kLiteralContextBits = 6;
var kDistanceContextBits = 2;

var HUFFMAN_TABLE_BITS = 8;
var HUFFMAN_TABLE_MASK = 0xff;
/* Maximum possible Huffman table size for an alphabet size of 704, max code
 * length 15 and root table bits 8. */
var HUFFMAN_MAX_TABLE_SIZE = 1080;

var CODE_LENGTH_CODES = 18;
var kCodeLengthCodeOrder = new Uint8Array([
  1, 2, 3, 4, 0, 5, 17, 6, 16, 7, 8, 9, 10, 11, 12, 13, 14, 15,
]);

var NUM_DISTANCE_SHORT_CODES = 16;
var kDistanceShortCodeIndexOffset = new Uint8Array([
  3, 2, 1, 0, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2
]);

var kDistanceShortCodeValueOffset = new Int8Array([
  0, 0, 0, 0, -1, 1, -2, 2, -3, 3, -1, 1, -2, 2, -3, 3
]);

var kMaxHuffmanTableSize = new Uint16Array([
  256, 402, 436, 468, 500, 534, 566, 598, 630, 662, 694, 726, 758, 790, 822,
  854, 886, 920, 952, 984, 1016, 1048, 1080
]);

function DecodeWindowBits(br) {
  var n;
  if (br.readBits(1) === 0) {
    return 16;
  }
  
  n = br.readBits(3);
  if (n > 0) {
    return 17 + n;
  }
  
  n = br.readBits(3);
  if (n > 0) {
    return 8 + n;
  }
  
  return 17;
}

/* Decodes a number in the range [0..255], by reading 1 - 11 bits. */
function DecodeVarLenUint8(br) {
  if (br.readBits(1)) {
    var nbits = br.readBits(3);
    if (nbits === 0) {
      return 1;
    } else {
      return br.readBits(nbits) + (1 << nbits);
    }
  }
  return 0;
}

function MetaBlockLength() {
  this.meta_block_length = 0;
  this.input_end = 0;
  this.is_uncompressed = 0;
  this.is_metadata = false;
}

function DecodeMetaBlockLength(br) {
  var out = new MetaBlockLength;  
  var size_nibbles;
  var size_bytes;
  var i;
  
  out.input_end = br.readBits(1);
  if (out.input_end && br.readBits(1)) {
    return out;
  }
  
  size_nibbles = br.readBits(2) + 4;
  if (size_nibbles === 7) {
    out.is_metadata = true;
    
    if (br.readBits(1) !== 0)
      throw new Error('Invalid reserved bit');
    
    size_bytes = br.readBits(2);
    if (size_bytes === 0)
      return out;
    
    for (i = 0; i < size_bytes; i++) {
      var next_byte = br.readBits(8);
      if (i + 1 === size_bytes && size_bytes > 1 && next_byte === 0)
        throw new Error('Invalid size byte');
      
      out.meta_block_length |= next_byte << (i * 8);
    }
  } else {
    for (i = 0; i < size_nibbles; ++i) {
      var next_nibble = br.readBits(4);
      if (i + 1 === size_nibbles && size_nibbles > 4 && next_nibble === 0)
        throw new Error('Invalid size nibble');
      
      out.meta_block_length |= next_nibble << (i * 4);
    }
  }
  
  ++out.meta_block_length;
  
  if (!out.input_end && !out.is_metadata) {
    out.is_uncompressed = br.readBits(1);
  }
  
  return out;
}

/* Decodes the next Huffman code from bit-stream. */
function ReadSymbol(table, index, br) {
  var start_index = index;
  
  var nbits;
  br.fillBitWindow();
  index += (br.val_ >>> br.bit_pos_) & HUFFMAN_TABLE_MASK;
  nbits = table[index].bits - HUFFMAN_TABLE_BITS;
  if (nbits > 0) {
    br.bit_pos_ += HUFFMAN_TABLE_BITS;
    index += table[index].value;
    index += (br.val_ >>> br.bit_pos_) & ((1 << nbits) - 1);
  }
  br.bit_pos_ += table[index].bits;
  return table[index].value;
}

function ReadHuffmanCodeLengths(code_length_code_lengths, num_symbols, code_lengths, br) {
  var symbol = 0;
  var prev_code_len = kDefaultCodeLength;
  var repeat = 0;
  var repeat_code_len = 0;
  var space = 32768;
  
  var table = [];
  for (var i = 0; i < 32; i++)
    table.push(new HuffmanCode(0, 0));
  
  BrotliBuildHuffmanTable(table, 0, 5, code_length_code_lengths, CODE_LENGTH_CODES);

  while (symbol < num_symbols && space > 0) {
    var p = 0;
    var code_len;
    
    br.readMoreInput();
    br.fillBitWindow();
    p += (br.val_ >>> br.bit_pos_) & 31;
    br.bit_pos_ += table[p].bits;
    code_len = table[p].value & 0xff;
    if (code_len < kCodeLengthRepeatCode) {
      repeat = 0;
      code_lengths[symbol++] = code_len;
      if (code_len !== 0) {
        prev_code_len = code_len;
        space -= 32768 >> code_len;
      }
    } else {
      var extra_bits = code_len - 14;
      var old_repeat;
      var repeat_delta;
      var new_len = 0;
      if (code_len === kCodeLengthRepeatCode) {
        new_len = prev_code_len;
      }
      if (repeat_code_len !== new_len) {
        repeat = 0;
        repeat_code_len = new_len;
      }
      old_repeat = repeat;
      if (repeat > 0) {
        repeat -= 2;
        repeat <<= extra_bits;
      }
      repeat += br.readBits(extra_bits) + 3;
      repeat_delta = repeat - old_repeat;
      if (symbol + repeat_delta > num_symbols) {
        throw new Error('[ReadHuffmanCodeLengths] symbol + repeat_delta > num_symbols');
      }
      
      for (var x = 0; x < repeat_delta; x++)
        code_lengths[symbol + x] = repeat_code_len;
      
      symbol += repeat_delta;
      
      if (repeat_code_len !== 0) {
        space -= repeat_delta << (15 - repeat_code_len);
      }
    }
  }
  if (space !== 0) {
    throw new Error("[ReadHuffmanCodeLengths] space = " + space);
  }
  
  for (; symbol < num_symbols; symbol++)
    code_lengths[symbol] = 0;
}

function ReadHuffmanCode(alphabet_size, tables, table, br) {
  var table_size = 0;
  var simple_code_or_skip;
  var code_lengths = new Uint8Array(alphabet_size);
  
  br.readMoreInput();
  
  /* simple_code_or_skip is used as follows:
     1 for simple code;
     0 for no skipping, 2 skips 2 code lengths, 3 skips 3 code lengths */
  simple_code_or_skip = br.readBits(2);
  if (simple_code_or_skip === 1) {
    /* Read symbols, codes & code lengths directly. */
    var i;
    var max_bits_counter = alphabet_size - 1;
    var max_bits = 0;
    var symbols = new Int32Array(4);
    var num_symbols = br.readBits(2) + 1;
    while (max_bits_counter) {
      max_bits_counter >>= 1;
      ++max_bits;
    }

    for (i = 0; i < num_symbols; ++i) {
      symbols[i] = br.readBits(max_bits) % alphabet_size;
      code_lengths[symbols[i]] = 2;
    }
    code_lengths[symbols[0]] = 1;
    switch (num_symbols) {
      case 1:
        break;
      case 3:
        if ((symbols[0] === symbols[1]) ||
            (symbols[0] === symbols[2]) ||
            (symbols[1] === symbols[2])) {
          throw new Error('[ReadHuffmanCode] invalid symbols');
        }
        break;
      case 2:
        if (symbols[0] === symbols[1]) {
          throw new Error('[ReadHuffmanCode] invalid symbols');
        }
        
        code_lengths[symbols[1]] = 1;
        break;
      case 4:
        if ((symbols[0] === symbols[1]) ||
            (symbols[0] === symbols[2]) ||
            (symbols[0] === symbols[3]) ||
            (symbols[1] === symbols[2]) ||
            (symbols[1] === symbols[3]) ||
            (symbols[2] === symbols[3])) {
          throw new Error('[ReadHuffmanCode] invalid symbols');
        }
        
        if (br.readBits(1)) {
          code_lengths[symbols[2]] = 3;
          code_lengths[symbols[3]] = 3;
        } else {
          code_lengths[symbols[0]] = 2;
        }
        break;
    }
  } else {  /* Decode Huffman-coded code lengths. */
    var i;
    var code_length_code_lengths = new Uint8Array(CODE_LENGTH_CODES);
    var space = 32;
    var num_codes = 0;
    /* Static Huffman code for the code length code lengths */
    var huff = [
      new HuffmanCode(2, 0), new HuffmanCode(2, 4), new HuffmanCode(2, 3), new HuffmanCode(3, 2), 
      new HuffmanCode(2, 0), new HuffmanCode(2, 4), new HuffmanCode(2, 3), new HuffmanCode(4, 1),
      new HuffmanCode(2, 0), new HuffmanCode(2, 4), new HuffmanCode(2, 3), new HuffmanCode(3, 2), 
      new HuffmanCode(2, 0), new HuffmanCode(2, 4), new HuffmanCode(2, 3), new HuffmanCode(4, 5)
    ];
    for (i = simple_code_or_skip; i < CODE_LENGTH_CODES && space > 0; ++i) {
      var code_len_idx = kCodeLengthCodeOrder[i];
      var p = 0;
      var v;
      br.fillBitWindow();
      p += (br.val_ >>> br.bit_pos_) & 15;
      br.bit_pos_ += huff[p].bits;
      v = huff[p].value;
      code_length_code_lengths[code_len_idx] = v;
      if (v !== 0) {
        space -= (32 >> v);
        ++num_codes;
      }
    }
    
    if (!(num_codes === 1 || space === 0))
      throw new Error('[ReadHuffmanCode] invalid num_codes or space');
    
    ReadHuffmanCodeLengths(code_length_code_lengths, alphabet_size, code_lengths, br);
  }
  
  table_size = BrotliBuildHuffmanTable(tables, table, HUFFMAN_TABLE_BITS, code_lengths, alphabet_size);
  
  if (table_size === 0) {
    throw new Error("[ReadHuffmanCode] BuildHuffmanTable failed: ");
  }
  
  return table_size;
}

function ReadBlockLength(table, index, br) {
  var code;
  var nbits;
  code = ReadSymbol(table, index, br);
  nbits = Prefix.kBlockLengthPrefixCode[code].nbits;
  return Prefix.kBlockLengthPrefixCode[code].offset + br.readBits(nbits);
}

function TranslateShortCodes(code, ringbuffer, index) {
  var val;
  if (code < NUM_DISTANCE_SHORT_CODES) {
    index += kDistanceShortCodeIndexOffset[code];
    index &= 3;
    val = ringbuffer[index] + kDistanceShortCodeValueOffset[code];
  } else {
    val = code - NUM_DISTANCE_SHORT_CODES + 1;
  }
  return val;
}

function MoveToFront(v, index) {
  var value = v[index];
  var i = index;
  for (; i; --i) v[i] = v[i - 1];
  v[0] = value;
}

function InverseMoveToFrontTransform(v, v_len) {
  var mtf = new Uint8Array(256);
  var i;
  for (i = 0; i < 256; ++i) {
    mtf[i] = i;
  }
  for (i = 0; i < v_len; ++i) {
    var index = v[i];
    v[i] = mtf[index];
    if (index) MoveToFront(mtf, index);
  }
}

/* Contains a collection of huffman trees with the same alphabet size. */
function HuffmanTreeGroup(alphabet_size, num_htrees) {
  this.alphabet_size = alphabet_size;
  this.num_htrees = num_htrees;
  this.codes = new Array(num_htrees + num_htrees * kMaxHuffmanTableSize[(alphabet_size + 31) >>> 5]);  
  this.htrees = new Uint32Array(num_htrees);
}

HuffmanTreeGroup.prototype.decode = function(br) {
  var i;
  var table_size;
  var next = 0;
  for (i = 0; i < this.num_htrees; ++i) {
    this.htrees[i] = next;
    table_size = ReadHuffmanCode(this.alphabet_size, this.codes, next, br);
    next += table_size;
  }
};

function DecodeContextMap(context_map_size, br) {
  var out = { num_htrees: null, context_map: null };
  var use_rle_for_zeros;
  var max_run_length_prefix = 0;
  var table;
  var i;
  
  br.readMoreInput();
  var num_htrees = out.num_htrees = DecodeVarLenUint8(br) + 1;

  var context_map = out.context_map = new Uint8Array(context_map_size);
  if (num_htrees <= 1) {
    return out;
  }

  use_rle_for_zeros = br.readBits(1);
  if (use_rle_for_zeros) {
    max_run_length_prefix = br.readBits(4) + 1;
  }
  
  table = [];
  for (i = 0; i < HUFFMAN_MAX_TABLE_SIZE; i++) {
    table[i] = new HuffmanCode(0, 0);
  }
  
  ReadHuffmanCode(num_htrees + max_run_length_prefix, table, 0, br);
  
  for (i = 0; i < context_map_size;) {
    var code;

    br.readMoreInput();
    code = ReadSymbol(table, 0, br);
    if (code === 0) {
      context_map[i] = 0;
      ++i;
    } else if (code <= max_run_length_prefix) {
      var reps = 1 + (1 << code) + br.readBits(code);
      while (--reps) {
        if (i >= context_map_size) {
          throw new Error("[DecodeContextMap] i >= context_map_size");
        }
        context_map[i] = 0;
        ++i;
      }
    } else {
      context_map[i] = code - max_run_length_prefix;
      ++i;
    }
  }
  if (br.readBits(1)) {
    InverseMoveToFrontTransform(context_map, context_map_size);
  }
  
  return out;
}

function DecodeBlockType(max_block_type, trees, tree_type, block_types, ringbuffers, indexes, br) {
  var ringbuffer = tree_type * 2;
  var index = tree_type;
  var type_code = ReadSymbol(trees, tree_type * HUFFMAN_MAX_TABLE_SIZE, br);
  var block_type;
  if (type_code === 0) {
    block_type = ringbuffers[ringbuffer + (indexes[index] & 1)];
  } else if (type_code === 1) {
    block_type = ringbuffers[ringbuffer + ((indexes[index] - 1) & 1)] + 1;
  } else {
    block_type = type_code - 2;
  }
  if (block_type >= max_block_type) {
    block_type -= max_block_type;
  }
  block_types[tree_type] = block_type;
  ringbuffers[ringbuffer + (indexes[index] & 1)] = block_type;
  ++indexes[index];
}

function CopyUncompressedBlockToOutput(output, len, pos, ringbuffer, ringbuffer_mask, br) {
  var rb_size = ringbuffer_mask + 1;
  var rb_pos = pos & ringbuffer_mask;
  var br_pos = br.pos_ & BrotliBitReader.IBUF_MASK;
  var nbytes;

  /* For short lengths copy byte-by-byte */
  if (len < 8 || br.bit_pos_ + (len << 3) < br.bit_end_pos_) {
    while (len-- > 0) {
      br.readMoreInput();
      ringbuffer[rb_pos++] = br.readBits(8);
      if (rb_pos === rb_size) {
        output.write(ringbuffer, rb_size);
        rb_pos = 0;
      }
    }
    return;
  }

  if (br.bit_end_pos_ < 32) {
    throw new Error('[CopyUncompressedBlockToOutput] br.bit_end_pos_ < 32');
  }

  /* Copy remaining 0-4 bytes from br.val_ to ringbuffer. */
  while (br.bit_pos_ < 32) {
    ringbuffer[rb_pos] = (br.val_ >>> br.bit_pos_);
    br.bit_pos_ += 8;
    ++rb_pos;
    --len;
  }

  /* Copy remaining bytes from br.buf_ to ringbuffer. */
  nbytes = (br.bit_end_pos_ - br.bit_pos_) >> 3;
  if (br_pos + nbytes > BrotliBitReader.IBUF_MASK) {
    var tail = BrotliBitReader.IBUF_MASK + 1 - br_pos;
    for (var x = 0; x < tail; x++)
      ringbuffer[rb_pos + x] = br.buf_[br_pos + x];
    
    nbytes -= tail;
    rb_pos += tail;
    len -= tail;
    br_pos = 0;
  }

  for (var x = 0; x < nbytes; x++)
    ringbuffer[rb_pos + x] = br.buf_[br_pos + x];
  
  rb_pos += nbytes;
  len -= nbytes;

  /* If we wrote past the logical end of the ringbuffer, copy the tail of the
     ringbuffer to its beginning and flush the ringbuffer to the output. */
  if (rb_pos >= rb_size) {
    output.write(ringbuffer, rb_size);
    rb_pos -= rb_size;    
    for (var x = 0; x < rb_pos; x++)
      ringbuffer[x] = ringbuffer[rb_size + x];
  }

  /* If we have more to copy than the remaining size of the ringbuffer, then we
     first fill the ringbuffer from the input and then flush the ringbuffer to
     the output */
  while (rb_pos + len >= rb_size) {
    nbytes = rb_size - rb_pos;
    if (br.input_.read(ringbuffer, rb_pos, nbytes) < nbytes) {
      throw new Error('[CopyUncompressedBlockToOutput] not enough bytes');
    }
    output.write(ringbuffer, rb_size);
    len -= nbytes;
    rb_pos = 0;
  }

  /* Copy straight from the input onto the ringbuffer. The ringbuffer will be
     flushed to the output at a later time. */
  if (br.input_.read(ringbuffer, rb_pos, len) < len) {
    throw new Error('[CopyUncompressedBlockToOutput] not enough bytes');
  }

  /* Restore the state of the bit reader. */
  br.reset();
}

/* Advances the bit reader position to the next byte boundary and verifies
   that any skipped bits are set to zero. */
function JumpToByteBoundary(br) {
  var new_bit_pos = (br.bit_pos_ + 7) & ~7;
  var pad_bits = br.readBits(new_bit_pos - br.bit_pos_);
  return pad_bits == 0;
}

function BrotliDecompressedSize(buffer) {
  var input = new BrotliInput(buffer);
  var br = new BrotliBitReader(input);
  DecodeWindowBits(br);
  var out = DecodeMetaBlockLength(br);
  return out.meta_block_length;
}

exports.BrotliDecompressedSize = BrotliDecompressedSize;

function BrotliDecompressBuffer(buffer, output_size) {
  var input = new BrotliInput(buffer);
  
  if (output_size == null) {
    output_size = BrotliDecompressedSize(buffer);
  }
  
  var output_buffer = new Uint8Array(output_size);
  var output = new BrotliOutput(output_buffer);
  
  BrotliDecompress(input, output);
  
  if (output.pos < output.buffer.length) {
    output.buffer = output.buffer.subarray(0, output.pos);
  }
  
  return output.buffer;
}

exports.BrotliDecompressBuffer = BrotliDecompressBuffer;

function BrotliDecompress(input, output) {
  var i;
  var pos = 0;
  var input_end = 0;
  var window_bits = 0;
  var max_backward_distance;
  var max_distance = 0;
  var ringbuffer_size;
  var ringbuffer_mask;
  var ringbuffer;
  var ringbuffer_end;
  /* This ring buffer holds a few past copy distances that will be used by */
  /* some special distance codes. */
  var dist_rb = [ 16, 15, 11, 4 ];
  var dist_rb_idx = 0;
  /* The previous 2 bytes used for context. */
  var prev_byte1 = 0;
  var prev_byte2 = 0;
  var hgroup = [new HuffmanTreeGroup(0, 0), new HuffmanTreeGroup(0, 0), new HuffmanTreeGroup(0, 0)];
  var block_type_trees;
  var block_len_trees;
  var br;

  /* We need the slack region for the following reasons:
       - always doing two 8-byte copies for fast backward copying
       - transforms
       - flushing the input ringbuffer when decoding uncompressed blocks */
  var kRingBufferWriteAheadSlack = 128 + BrotliBitReader.READ_SIZE;

  br = new BrotliBitReader(input);

  /* Decode window size. */
  window_bits = DecodeWindowBits(br);
  max_backward_distance = (1 << window_bits) - 16;

  ringbuffer_size = 1 << window_bits;
  ringbuffer_mask = ringbuffer_size - 1;
  ringbuffer = new Uint8Array(ringbuffer_size + kRingBufferWriteAheadSlack + BrotliDictionary.maxDictionaryWordLength);
  ringbuffer_end = ringbuffer_size;

  block_type_trees = [];
  block_len_trees = [];
  for (var x = 0; x < 3 * HUFFMAN_MAX_TABLE_SIZE; x++) {
    block_type_trees[x] = new HuffmanCode(0, 0);
    block_len_trees[x] = new HuffmanCode(0, 0);
  }

  while (!input_end) {
    var meta_block_remaining_len = 0;
    var is_uncompressed;
    var block_length = [ 1 << 28, 1 << 28, 1 << 28 ];
    var block_type = [ 0 ];
    var num_block_types = [ 1, 1, 1 ];
    var block_type_rb = [ 0, 1, 0, 1, 0, 1 ];
    var block_type_rb_index = [ 0 ];
    var distance_postfix_bits;
    var num_direct_distance_codes;
    var distance_postfix_mask;
    var num_distance_codes;
    var context_map = null;
    var context_modes = null;
    var num_literal_htrees;
    var dist_context_map = null;
    var num_dist_htrees;
    var context_offset = 0;
    var context_map_slice = null;
    var literal_htree_index = 0;
    var dist_context_offset = 0;
    var dist_context_map_slice = null;
    var dist_htree_index = 0;
    var context_lookup_offset1 = 0;
    var context_lookup_offset2 = 0;
    var context_mode;
    var htree_command;

    for (i = 0; i < 3; ++i) {
      hgroup[i].codes = null;
      hgroup[i].htrees = null;
    }

    br.readMoreInput();
    
    var _out = DecodeMetaBlockLength(br);
    meta_block_remaining_len = _out.meta_block_length;
    if (pos + meta_block_remaining_len > output.buffer.length) {
      /* We need to grow the output buffer to fit the additional data. */
      var tmp = new Uint8Array( pos + meta_block_remaining_len );
      tmp.set( output.buffer );
      output.buffer = tmp;
    }    
    input_end = _out.input_end;
    is_uncompressed = _out.is_uncompressed;
    
    if (_out.is_metadata) {
      JumpToByteBoundary(br);
      
      for (; meta_block_remaining_len > 0; --meta_block_remaining_len) {
        br.readMoreInput();
        /* Read one byte and ignore it. */
        br.readBits(8);
      }
      
      continue;
    }
    
    if (meta_block_remaining_len === 0) {
      continue;
    }
    
    if (is_uncompressed) {
      br.bit_pos_ = (br.bit_pos_ + 7) & ~7;
      CopyUncompressedBlockToOutput(output, meta_block_remaining_len, pos,
                                    ringbuffer, ringbuffer_mask, br);
      pos += meta_block_remaining_len;
      continue;
    }
    
    for (i = 0; i < 3; ++i) {
      num_block_types[i] = DecodeVarLenUint8(br) + 1;
      if (num_block_types[i] >= 2) {
        ReadHuffmanCode(num_block_types[i] + 2, block_type_trees, i * HUFFMAN_MAX_TABLE_SIZE, br);
        ReadHuffmanCode(kNumBlockLengthCodes, block_len_trees, i * HUFFMAN_MAX_TABLE_SIZE, br);
        block_length[i] = ReadBlockLength(block_len_trees, i * HUFFMAN_MAX_TABLE_SIZE, br);
        block_type_rb_index[i] = 1;
      }
    }
    
    br.readMoreInput();
    
    distance_postfix_bits = br.readBits(2);
    num_direct_distance_codes = NUM_DISTANCE_SHORT_CODES + (br.readBits(4) << distance_postfix_bits);
    distance_postfix_mask = (1 << distance_postfix_bits) - 1;
    num_distance_codes = (num_direct_distance_codes + (48 << distance_postfix_bits));
    context_modes = new Uint8Array(num_block_types[0]);

    for (i = 0; i < num_block_types[0]; ++i) {
       br.readMoreInput();
       context_modes[i] = (br.readBits(2) << 1);
    }
    
    var _o1 = DecodeContextMap(num_block_types[0] << kLiteralContextBits, br);
    num_literal_htrees = _o1.num_htrees;
    context_map = _o1.context_map;
    
    var _o2 = DecodeContextMap(num_block_types[2] << kDistanceContextBits, br);
    num_dist_htrees = _o2.num_htrees;
    dist_context_map = _o2.context_map;
    
    hgroup[0] = new HuffmanTreeGroup(kNumLiteralCodes, num_literal_htrees);
    hgroup[1] = new HuffmanTreeGroup(kNumInsertAndCopyCodes, num_block_types[1]);
    hgroup[2] = new HuffmanTreeGroup(num_distance_codes, num_dist_htrees);

    for (i = 0; i < 3; ++i) {
      hgroup[i].decode(br);
    }

    context_map_slice = 0;
    dist_context_map_slice = 0;
    context_mode = context_modes[block_type[0]];
    context_lookup_offset1 = Context.lookupOffsets[context_mode];
    context_lookup_offset2 = Context.lookupOffsets[context_mode + 1];
    htree_command = hgroup[1].htrees[0];

    while (meta_block_remaining_len > 0) {
      var cmd_code;
      var range_idx;
      var insert_code;
      var copy_code;
      var insert_length;
      var copy_length;
      var distance_code;
      var distance;
      var context;
      var j;
      var copy_dst;

      br.readMoreInput();
      
      if (block_length[1] === 0) {
        DecodeBlockType(num_block_types[1],
                        block_type_trees, 1, block_type, block_type_rb,
                        block_type_rb_index, br);
        block_length[1] = ReadBlockLength(block_len_trees, HUFFMAN_MAX_TABLE_SIZE, br);
        htree_command = hgroup[1].htrees[block_type[1]];
      }
      --block_length[1];
      cmd_code = ReadSymbol(hgroup[1].codes, htree_command, br);
      range_idx = cmd_code >> 6;
      if (range_idx >= 2) {
        range_idx -= 2;
        distance_code = -1;
      } else {
        distance_code = 0;
      }
      insert_code = Prefix.kInsertRangeLut[range_idx] + ((cmd_code >> 3) & 7);
      copy_code = Prefix.kCopyRangeLut[range_idx] + (cmd_code & 7);
      insert_length = Prefix.kInsertLengthPrefixCode[insert_code].offset +
          br.readBits(Prefix.kInsertLengthPrefixCode[insert_code].nbits);
      copy_length = Prefix.kCopyLengthPrefixCode[copy_code].offset +
          br.readBits(Prefix.kCopyLengthPrefixCode[copy_code].nbits);
      prev_byte1 = ringbuffer[pos-1 & ringbuffer_mask];
      prev_byte2 = ringbuffer[pos-2 & ringbuffer_mask];
      for (j = 0; j < insert_length; ++j) {
        br.readMoreInput();

        if (block_length[0] === 0) {
          DecodeBlockType(num_block_types[0],
                          block_type_trees, 0, block_type, block_type_rb,
                          block_type_rb_index, br);
          block_length[0] = ReadBlockLength(block_len_trees, 0, br);
          context_offset = block_type[0] << kLiteralContextBits;
          context_map_slice = context_offset;
          context_mode = context_modes[block_type[0]];
          context_lookup_offset1 = Context.lookupOffsets[context_mode];
          context_lookup_offset2 = Context.lookupOffsets[context_mode + 1];
        }
        context = (Context.lookup[context_lookup_offset1 + prev_byte1] |
                   Context.lookup[context_lookup_offset2 + prev_byte2]);
        literal_htree_index = context_map[context_map_slice + context];
        --block_length[0];
        prev_byte2 = prev_byte1;
        prev_byte1 = ReadSymbol(hgroup[0].codes, hgroup[0].htrees[literal_htree_index], br);
        ringbuffer[pos & ringbuffer_mask] = prev_byte1;
        if ((pos & ringbuffer_mask) === ringbuffer_mask) {
          output.write(ringbuffer, ringbuffer_size);
        }
        ++pos;
      }
      meta_block_remaining_len -= insert_length;
      if (meta_block_remaining_len <= 0) break;

      if (distance_code < 0) {
        var context;
        
        br.readMoreInput();
        if (block_length[2] === 0) {
          DecodeBlockType(num_block_types[2],
                          block_type_trees, 2, block_type, block_type_rb,
                          block_type_rb_index, br);
          block_length[2] = ReadBlockLength(block_len_trees, 2 * HUFFMAN_MAX_TABLE_SIZE, br);
          dist_context_offset = block_type[2] << kDistanceContextBits;
          dist_context_map_slice = dist_context_offset;
        }
        --block_length[2];
        context = (copy_length > 4 ? 3 : copy_length - 2) & 0xff;
        dist_htree_index = dist_context_map[dist_context_map_slice + context];
        distance_code = ReadSymbol(hgroup[2].codes, hgroup[2].htrees[dist_htree_index], br);
        if (distance_code >= num_direct_distance_codes) {
          var nbits;
          var postfix;
          var offset;
          distance_code -= num_direct_distance_codes;
          postfix = distance_code & distance_postfix_mask;
          distance_code >>= distance_postfix_bits;
          nbits = (distance_code >> 1) + 1;
          offset = ((2 + (distance_code & 1)) << nbits) - 4;
          distance_code = num_direct_distance_codes +
              ((offset + br.readBits(nbits)) <<
               distance_postfix_bits) + postfix;
        }
      }

      /* Convert the distance code to the actual distance by possibly looking */
      /* up past distnaces from the ringbuffer. */
      distance = TranslateShortCodes(distance_code, dist_rb, dist_rb_idx);
      if (distance < 0) {
        throw new Error('[BrotliDecompress] invalid distance');
      }

      if (pos < max_backward_distance &&
          max_distance !== max_backward_distance) {
        max_distance = pos;
      } else {
        max_distance = max_backward_distance;
      }

      copy_dst = pos & ringbuffer_mask;

      if (distance > max_distance) {
        if (copy_length >= BrotliDictionary.minDictionaryWordLength &&
            copy_length <= BrotliDictionary.maxDictionaryWordLength) {
          var offset = BrotliDictionary.offsetsByLength[copy_length];
          var word_id = distance - max_distance - 1;
          var shift = BrotliDictionary.sizeBitsByLength[copy_length];
          var mask = (1 << shift) - 1;
          var word_idx = word_id & mask;
          var transform_idx = word_id >> shift;
          offset += word_idx * copy_length;
          if (transform_idx < Transform.kNumTransforms) {
            var len = Transform.transformDictionaryWord(ringbuffer, copy_dst, offset, copy_length, transform_idx);
            copy_dst += len;
            pos += len;
            meta_block_remaining_len -= len;
            if (copy_dst >= ringbuffer_end) {
              output.write(ringbuffer, ringbuffer_size);
              
              for (var _x = 0; _x < (copy_dst - ringbuffer_end); _x++)
                ringbuffer[_x] = ringbuffer[ringbuffer_end + _x];
            }
          } else {
            throw new Error("Invalid backward reference. pos: " + pos + " distance: " + distance +
              " len: " + copy_length + " bytes left: " + meta_block_remaining_len);
          }
        } else {
          throw new Error("Invalid backward reference. pos: " + pos + " distance: " + distance +
            " len: " + copy_length + " bytes left: " + meta_block_remaining_len);
        }
      } else {
        if (distance_code > 0) {
          dist_rb[dist_rb_idx & 3] = distance;
          ++dist_rb_idx;
        }

        if (copy_length > meta_block_remaining_len) {
          throw new Error("Invalid backward reference. pos: " + pos + " distance: " + distance +
            " len: " + copy_length + " bytes left: " + meta_block_remaining_len);
        }

        for (j = 0; j < copy_length; ++j) {
          ringbuffer[pos & ringbuffer_mask] = ringbuffer[(pos - distance) & ringbuffer_mask];
          if ((pos & ringbuffer_mask) === ringbuffer_mask) {
            output.write(ringbuffer, ringbuffer_size);
          }
          ++pos;
          --meta_block_remaining_len;
        }
      }

      /* When we get here, we must have inserted at least one literal and */
      /* made a copy of at least length two, therefore accessing the last 2 */
      /* bytes is valid. */
      prev_byte1 = ringbuffer[(pos - 1) & ringbuffer_mask];
      prev_byte2 = ringbuffer[(pos - 2) & ringbuffer_mask];
    }

    /* Protect pos from overflow, wrap it around at every GB of input data */
    pos &= 0x3fffffff;
  }

  output.write(ringbuffer, pos & ringbuffer_mask);
}

exports.BrotliDecompress = BrotliDecompress;

BrotliDictionary.init();
