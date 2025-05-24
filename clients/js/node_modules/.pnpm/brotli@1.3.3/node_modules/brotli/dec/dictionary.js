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

   Collection of static dictionary words.
*/

var data = require('./dictionary-data');
exports.init = function() {
  exports.dictionary = data.init();
};

exports.offsetsByLength = new Uint32Array([
     0,     0,     0,     0,     0,  4096,  9216, 21504, 35840, 44032,
 53248, 63488, 74752, 87040, 93696, 100864, 104704, 106752, 108928, 113536,
 115968, 118528, 119872, 121280, 122016,
]);

exports.sizeBitsByLength = new Uint8Array([
  0,  0,  0,  0, 10, 10, 11, 11, 10, 10,
 10, 10, 10,  9,  9,  8,  7,  7,  8,  7,
  7,  6,  6,  5,  5,
]);

exports.minDictionaryWordLength = 4;
exports.maxDictionaryWordLength = 24;
