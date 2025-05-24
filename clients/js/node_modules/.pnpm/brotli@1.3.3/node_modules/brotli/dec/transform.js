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

   Transformations on dictionary words.
*/

var BrotliDictionary = require('./dictionary');

var kIdentity       = 0;
var kOmitLast1      = 1;
var kOmitLast2      = 2;
var kOmitLast3      = 3;
var kOmitLast4      = 4;
var kOmitLast5      = 5;
var kOmitLast6      = 6;
var kOmitLast7      = 7;
var kOmitLast8      = 8;
var kOmitLast9      = 9;
var kUppercaseFirst = 10;
var kUppercaseAll   = 11;
var kOmitFirst1     = 12;
var kOmitFirst2     = 13;
var kOmitFirst3     = 14;
var kOmitFirst4     = 15;
var kOmitFirst5     = 16;
var kOmitFirst6     = 17;
var kOmitFirst7     = 18;
var kOmitFirst8     = 19;
var kOmitFirst9     = 20;

function Transform(prefix, transform, suffix) {
  this.prefix = new Uint8Array(prefix.length);
  this.transform = transform;
  this.suffix = new Uint8Array(suffix.length);
  
  for (var i = 0; i < prefix.length; i++)
    this.prefix[i] = prefix.charCodeAt(i);
  
  for (var i = 0; i < suffix.length; i++)
    this.suffix[i] = suffix.charCodeAt(i);
}

var kTransforms = [
     new Transform(         "", kIdentity,       ""           ),
     new Transform(         "", kIdentity,       " "          ),
     new Transform(        " ", kIdentity,       " "          ),
     new Transform(         "", kOmitFirst1,     ""           ),
     new Transform(         "", kUppercaseFirst, " "          ),
     new Transform(         "", kIdentity,       " the "      ),
     new Transform(        " ", kIdentity,       ""           ),
     new Transform(       "s ", kIdentity,       " "          ),
     new Transform(         "", kIdentity,       " of "       ),
     new Transform(         "", kUppercaseFirst, ""           ),
     new Transform(         "", kIdentity,       " and "      ),
     new Transform(         "", kOmitFirst2,     ""           ),
     new Transform(         "", kOmitLast1,      ""           ),
     new Transform(       ", ", kIdentity,       " "          ),
     new Transform(         "", kIdentity,       ", "         ),
     new Transform(        " ", kUppercaseFirst, " "          ),
     new Transform(         "", kIdentity,       " in "       ),
     new Transform(         "", kIdentity,       " to "       ),
     new Transform(       "e ", kIdentity,       " "          ),
     new Transform(         "", kIdentity,       "\""         ),
     new Transform(         "", kIdentity,       "."          ),
     new Transform(         "", kIdentity,       "\">"        ),
     new Transform(         "", kIdentity,       "\n"         ),
     new Transform(         "", kOmitLast3,      ""           ),
     new Transform(         "", kIdentity,       "]"          ),
     new Transform(         "", kIdentity,       " for "      ),
     new Transform(         "", kOmitFirst3,     ""           ),
     new Transform(         "", kOmitLast2,      ""           ),
     new Transform(         "", kIdentity,       " a "        ),
     new Transform(         "", kIdentity,       " that "     ),
     new Transform(        " ", kUppercaseFirst, ""           ),
     new Transform(         "", kIdentity,       ". "         ),
     new Transform(        ".", kIdentity,       ""           ),
     new Transform(        " ", kIdentity,       ", "         ),
     new Transform(         "", kOmitFirst4,     ""           ),
     new Transform(         "", kIdentity,       " with "     ),
     new Transform(         "", kIdentity,       "'"          ),
     new Transform(         "", kIdentity,       " from "     ),
     new Transform(         "", kIdentity,       " by "       ),
     new Transform(         "", kOmitFirst5,     ""           ),
     new Transform(         "", kOmitFirst6,     ""           ),
     new Transform(    " the ", kIdentity,       ""           ),
     new Transform(         "", kOmitLast4,      ""           ),
     new Transform(         "", kIdentity,       ". The "     ),
     new Transform(         "", kUppercaseAll,   ""           ),
     new Transform(         "", kIdentity,       " on "       ),
     new Transform(         "", kIdentity,       " as "       ),
     new Transform(         "", kIdentity,       " is "       ),
     new Transform(         "", kOmitLast7,      ""           ),
     new Transform(         "", kOmitLast1,      "ing "       ),
     new Transform(         "", kIdentity,       "\n\t"       ),
     new Transform(         "", kIdentity,       ":"          ),
     new Transform(        " ", kIdentity,       ". "         ),
     new Transform(         "", kIdentity,       "ed "        ),
     new Transform(         "", kOmitFirst9,     ""           ),
     new Transform(         "", kOmitFirst7,     ""           ),
     new Transform(         "", kOmitLast6,      ""           ),
     new Transform(         "", kIdentity,       "("          ),
     new Transform(         "", kUppercaseFirst, ", "         ),
     new Transform(         "", kOmitLast8,      ""           ),
     new Transform(         "", kIdentity,       " at "       ),
     new Transform(         "", kIdentity,       "ly "        ),
     new Transform(    " the ", kIdentity,       " of "       ),
     new Transform(         "", kOmitLast5,      ""           ),
     new Transform(         "", kOmitLast9,      ""           ),
     new Transform(        " ", kUppercaseFirst, ", "         ),
     new Transform(         "", kUppercaseFirst, "\""         ),
     new Transform(        ".", kIdentity,       "("          ),
     new Transform(         "", kUppercaseAll,   " "          ),
     new Transform(         "", kUppercaseFirst, "\">"        ),
     new Transform(         "", kIdentity,       "=\""        ),
     new Transform(        " ", kIdentity,       "."          ),
     new Transform(    ".com/", kIdentity,       ""           ),
     new Transform(    " the ", kIdentity,       " of the "   ),
     new Transform(         "", kUppercaseFirst, "'"          ),
     new Transform(         "", kIdentity,       ". This "    ),
     new Transform(         "", kIdentity,       ","          ),
     new Transform(        ".", kIdentity,       " "          ),
     new Transform(         "", kUppercaseFirst, "("          ),
     new Transform(         "", kUppercaseFirst, "."          ),
     new Transform(         "", kIdentity,       " not "      ),
     new Transform(        " ", kIdentity,       "=\""        ),
     new Transform(         "", kIdentity,       "er "        ),
     new Transform(        " ", kUppercaseAll,   " "          ),
     new Transform(         "", kIdentity,       "al "        ),
     new Transform(        " ", kUppercaseAll,   ""           ),
     new Transform(         "", kIdentity,       "='"         ),
     new Transform(         "", kUppercaseAll,   "\""         ),
     new Transform(         "", kUppercaseFirst, ". "         ),
     new Transform(        " ", kIdentity,       "("          ),
     new Transform(         "", kIdentity,       "ful "       ),
     new Transform(        " ", kUppercaseFirst, ". "         ),
     new Transform(         "", kIdentity,       "ive "       ),
     new Transform(         "", kIdentity,       "less "      ),
     new Transform(         "", kUppercaseAll,   "'"          ),
     new Transform(         "", kIdentity,       "est "       ),
     new Transform(        " ", kUppercaseFirst, "."          ),
     new Transform(         "", kUppercaseAll,   "\">"        ),
     new Transform(        " ", kIdentity,       "='"         ),
     new Transform(         "", kUppercaseFirst, ","          ),
     new Transform(         "", kIdentity,       "ize "       ),
     new Transform(         "", kUppercaseAll,   "."          ),
     new Transform( "\xc2\xa0", kIdentity,       ""           ),
     new Transform(        " ", kIdentity,       ","          ),
     new Transform(         "", kUppercaseFirst, "=\""        ),
     new Transform(         "", kUppercaseAll,   "=\""        ),
     new Transform(         "", kIdentity,       "ous "       ),
     new Transform(         "", kUppercaseAll,   ", "         ),
     new Transform(         "", kUppercaseFirst, "='"         ),
     new Transform(        " ", kUppercaseFirst, ","          ),
     new Transform(        " ", kUppercaseAll,   "=\""        ),
     new Transform(        " ", kUppercaseAll,   ", "         ),
     new Transform(         "", kUppercaseAll,   ","          ),
     new Transform(         "", kUppercaseAll,   "("          ),
     new Transform(         "", kUppercaseAll,   ". "         ),
     new Transform(        " ", kUppercaseAll,   "."          ),
     new Transform(         "", kUppercaseAll,   "='"         ),
     new Transform(        " ", kUppercaseAll,   ". "         ),
     new Transform(        " ", kUppercaseFirst, "=\""        ),
     new Transform(        " ", kUppercaseAll,   "='"         ),
     new Transform(        " ", kUppercaseFirst, "='"         )
];

exports.kTransforms = kTransforms;
exports.kNumTransforms = kTransforms.length;

function ToUpperCase(p, i) {
  if (p[i] < 0xc0) {
    if (p[i] >= 97 && p[i] <= 122) {
      p[i] ^= 32;
    }
    return 1;
  }
  
  /* An overly simplified uppercasing model for utf-8. */
  if (p[i] < 0xe0) {
    p[i + 1] ^= 32;
    return 2;
  }
  
  /* An arbitrary transform for three byte characters. */
  p[i + 2] ^= 5;
  return 3;
}

exports.transformDictionaryWord = function(dst, idx, word, len, transform) {
  var prefix = kTransforms[transform].prefix;
  var suffix = kTransforms[transform].suffix;
  var t = kTransforms[transform].transform;
  var skip = t < kOmitFirst1 ? 0 : t - (kOmitFirst1 - 1);
  var i = 0;
  var start_idx = idx;
  var uppercase;
  
  if (skip > len) {
    skip = len;
  }
  
  var prefix_pos = 0;
  while (prefix_pos < prefix.length) {
    dst[idx++] = prefix[prefix_pos++];
  }
  
  word += skip;
  len -= skip;
  
  if (t <= kOmitLast9) {
    len -= t;
  }
  
  for (i = 0; i < len; i++) {
    dst[idx++] = BrotliDictionary.dictionary[word + i];
  }
  
  uppercase = idx - len;
  
  if (t === kUppercaseFirst) {
    ToUpperCase(dst, uppercase);
  } else if (t === kUppercaseAll) {
    while (len > 0) {
      var step = ToUpperCase(dst, uppercase);
      uppercase += step;
      len -= step;
    }
  }
  
  var suffix_pos = 0;
  while (suffix_pos < suffix.length) {
    dst[idx++] = suffix[suffix_pos++];
  }
  
  return idx - start_idx;
}
