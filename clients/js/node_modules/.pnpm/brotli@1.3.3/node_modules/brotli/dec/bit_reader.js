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

   Bit reading helpers
*/

var BROTLI_READ_SIZE = 4096;
var BROTLI_IBUF_SIZE =  (2 * BROTLI_READ_SIZE + 32);
var BROTLI_IBUF_MASK =  (2 * BROTLI_READ_SIZE - 1);

var kBitMask = new Uint32Array([
  0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767,
  65535, 131071, 262143, 524287, 1048575, 2097151, 4194303, 8388607, 16777215
]);

/* Input byte buffer, consist of a ringbuffer and a "slack" region where */
/* bytes from the start of the ringbuffer are copied. */
function BrotliBitReader(input) {
  this.buf_ = new Uint8Array(BROTLI_IBUF_SIZE);
  this.input_ = input;    /* input callback */
  
  this.reset();
}

BrotliBitReader.READ_SIZE = BROTLI_READ_SIZE;
BrotliBitReader.IBUF_MASK = BROTLI_IBUF_MASK;

BrotliBitReader.prototype.reset = function() {
  this.buf_ptr_ = 0;      /* next input will write here */
  this.val_ = 0;          /* pre-fetched bits */
  this.pos_ = 0;          /* byte position in stream */
  this.bit_pos_ = 0;      /* current bit-reading position in val_ */
  this.bit_end_pos_ = 0;  /* bit-reading end position from LSB of val_ */
  this.eos_ = 0;          /* input stream is finished */
  
  this.readMoreInput();
  for (var i = 0; i < 4; i++) {
    this.val_ |= this.buf_[this.pos_] << (8 * i);
    ++this.pos_;
  }
  
  return this.bit_end_pos_ > 0;
};

/* Fills up the input ringbuffer by calling the input callback.

   Does nothing if there are at least 32 bytes present after current position.

   Returns 0 if either:
    - the input callback returned an error, or
    - there is no more input and the position is past the end of the stream.

   After encountering the end of the input stream, 32 additional zero bytes are
   copied to the ringbuffer, therefore it is safe to call this function after
   every 32 bytes of input is read.
*/
BrotliBitReader.prototype.readMoreInput = function() {
  if (this.bit_end_pos_ > 256) {
    return;
  } else if (this.eos_) {
    if (this.bit_pos_ > this.bit_end_pos_)
      throw new Error('Unexpected end of input ' + this.bit_pos_ + ' ' + this.bit_end_pos_);
  } else {
    var dst = this.buf_ptr_;
    var bytes_read = this.input_.read(this.buf_, dst, BROTLI_READ_SIZE);
    if (bytes_read < 0) {
      throw new Error('Unexpected end of input');
    }
    
    if (bytes_read < BROTLI_READ_SIZE) {
      this.eos_ = 1;
      /* Store 32 bytes of zero after the stream end. */
      for (var p = 0; p < 32; p++)
        this.buf_[dst + bytes_read + p] = 0;
    }
    
    if (dst === 0) {
      /* Copy the head of the ringbuffer to the slack region. */
      for (var p = 0; p < 32; p++)
        this.buf_[(BROTLI_READ_SIZE << 1) + p] = this.buf_[p];

      this.buf_ptr_ = BROTLI_READ_SIZE;
    } else {
      this.buf_ptr_ = 0;
    }
    
    this.bit_end_pos_ += bytes_read << 3;
  }
};

/* Guarantees that there are at least 24 bits in the buffer. */
BrotliBitReader.prototype.fillBitWindow = function() {    
  while (this.bit_pos_ >= 8) {
    this.val_ >>>= 8;
    this.val_ |= this.buf_[this.pos_ & BROTLI_IBUF_MASK] << 24;
    ++this.pos_;
    this.bit_pos_ = this.bit_pos_ - 8 >>> 0;
    this.bit_end_pos_ = this.bit_end_pos_ - 8 >>> 0;
  }
};

/* Reads the specified number of bits from Read Buffer. */
BrotliBitReader.prototype.readBits = function(n_bits) {
  if (32 - this.bit_pos_ < n_bits) {
    this.fillBitWindow();
  }
  
  var val = ((this.val_ >>> this.bit_pos_) & kBitMask[n_bits]);
  this.bit_pos_ += n_bits;
  return val;
};

module.exports = BrotliBitReader;
