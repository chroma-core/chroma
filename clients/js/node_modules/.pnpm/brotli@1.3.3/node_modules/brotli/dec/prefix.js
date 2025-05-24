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

   Lookup tables to map prefix codes to value ranges. This is used during
   decoding of the block lengths, literal insertion lengths and copy lengths.
*/

/* Represents the range of values belonging to a prefix code: */
/* [offset, offset + 2^nbits) */
function PrefixCodeRange(offset, nbits) {
  this.offset = offset;
  this.nbits = nbits;
}

exports.kBlockLengthPrefixCode = [
  new PrefixCodeRange(1, 2), new PrefixCodeRange(5, 2), new PrefixCodeRange(9, 2), new PrefixCodeRange(13, 2),
  new PrefixCodeRange(17, 3), new PrefixCodeRange(25, 3), new PrefixCodeRange(33, 3), new PrefixCodeRange(41, 3),
  new PrefixCodeRange(49, 4), new PrefixCodeRange(65, 4), new PrefixCodeRange(81, 4), new PrefixCodeRange(97, 4),
  new PrefixCodeRange(113, 5), new PrefixCodeRange(145, 5), new PrefixCodeRange(177, 5), new PrefixCodeRange(209, 5),
  new PrefixCodeRange(241, 6), new PrefixCodeRange(305, 6), new PrefixCodeRange(369, 7), new PrefixCodeRange(497, 8),
  new PrefixCodeRange(753, 9), new PrefixCodeRange(1265, 10), new PrefixCodeRange(2289, 11), new PrefixCodeRange(4337, 12),
  new PrefixCodeRange(8433, 13), new PrefixCodeRange(16625, 24)
];

exports.kInsertLengthPrefixCode = [
  new PrefixCodeRange(0, 0), new PrefixCodeRange(1, 0), new PrefixCodeRange(2, 0), new PrefixCodeRange(3, 0),
  new PrefixCodeRange(4, 0), new PrefixCodeRange(5, 0), new PrefixCodeRange(6, 1), new PrefixCodeRange(8, 1),
  new PrefixCodeRange(10, 2), new PrefixCodeRange(14, 2), new PrefixCodeRange(18, 3), new PrefixCodeRange(26, 3),
  new PrefixCodeRange(34, 4), new PrefixCodeRange(50, 4), new PrefixCodeRange(66, 5), new PrefixCodeRange(98, 5),
  new PrefixCodeRange(130, 6), new PrefixCodeRange(194, 7), new PrefixCodeRange(322, 8), new PrefixCodeRange(578, 9),
  new PrefixCodeRange(1090, 10), new PrefixCodeRange(2114, 12), new PrefixCodeRange(6210, 14), new PrefixCodeRange(22594, 24),
];

exports.kCopyLengthPrefixCode = [
  new PrefixCodeRange(2, 0), new PrefixCodeRange(3, 0), new PrefixCodeRange(4, 0), new PrefixCodeRange(5, 0),
  new PrefixCodeRange(6, 0), new PrefixCodeRange(7, 0), new PrefixCodeRange(8, 0), new PrefixCodeRange(9, 0),
  new PrefixCodeRange(10, 1), new PrefixCodeRange(12, 1), new PrefixCodeRange(14, 2), new PrefixCodeRange(18, 2),
  new PrefixCodeRange(22, 3), new PrefixCodeRange(30, 3), new PrefixCodeRange(38, 4), new PrefixCodeRange(54, 4),
  new PrefixCodeRange(70, 5), new PrefixCodeRange(102, 5), new PrefixCodeRange(134, 6), new PrefixCodeRange(198, 7),
  new PrefixCodeRange(326, 8), new PrefixCodeRange(582, 9), new PrefixCodeRange(1094, 10), new PrefixCodeRange(2118, 24),
];

exports.kInsertRangeLut = [
  0, 0, 8, 8, 0, 16, 8, 16, 16,
];

exports.kCopyRangeLut = [
  0, 8, 0, 8, 16, 0, 16, 8, 16,
];
