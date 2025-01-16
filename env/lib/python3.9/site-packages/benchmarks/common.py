# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import codecs
import decimal
from functools import partial
import itertools
import sys
import unicodedata

import numpy as np

import pyarrow as pa

KILOBYTE = 1 << 10
MEGABYTE = KILOBYTE * KILOBYTE

DEFAULT_NONE_PROB = 0.3


def _multiplicate_sequence(base, target_size):
    q, r = divmod(target_size, len(base))
    return [base] * q + [base[:r]]


def get_random_bytes(n, seed=42):
    """
    Generate a random bytes object of size *n*.
    Note the result might be compressible.
    """
    rnd = np.random.RandomState(seed)
    # Computing a huge random bytestring can be costly, so we get at most
    # 100KB and duplicate the result as needed
    base_size = 100003
    q, r = divmod(n, base_size)
    if q == 0:
        result = rnd.bytes(r)
    else:
        base = rnd.bytes(base_size)
        result = b''.join(_multiplicate_sequence(base, n))
    assert len(result) == n
    return result


def get_random_ascii(n, seed=42):
    """
    Get a random ASCII-only unicode string of size *n*.
    """
    arr = np.frombuffer(get_random_bytes(n, seed=seed), dtype=np.int8) & 0x7f
    result, _ = codecs.ascii_decode(arr)
    assert isinstance(result, str)
    assert len(result) == n
    return result


def _random_unicode_letters(n, seed=42):
    """
    Generate a string of random unicode letters (slow).
    """
    def _get_more_candidates():
        return rnd.randint(0, sys.maxunicode, size=n).tolist()

    rnd = np.random.RandomState(seed)
    out = []
    candidates = []

    while len(out) < n:
        if not candidates:
            candidates = _get_more_candidates()
        ch = chr(candidates.pop())
        # XXX Do we actually care that the code points are valid?
        if unicodedata.category(ch)[0] == 'L':
            out.append(ch)
    return out


_1024_random_unicode_letters = _random_unicode_letters(1024)


def get_random_unicode(n, seed=42):
    """
    Get a random non-ASCII unicode string of size *n*.
    """
    indices = np.frombuffer(get_random_bytes(n * 2, seed=seed),
                            dtype=np.int16) & 1023
    unicode_arr = np.array(_1024_random_unicode_letters)[indices]

    result = ''.join(unicode_arr.tolist())
    assert len(result) == n, (len(result), len(unicode_arr))
    return result


class BuiltinsGenerator(object):

    def __init__(self, seed=42):
        self.rnd = np.random.RandomState(seed)

    def sprinkle(self, lst, prob, value):
        """
        Sprinkle *value* entries in list *lst* with likelihood *prob*.
        """
        for i, p in enumerate(self.rnd.random_sample(size=len(lst))):
            if p < prob:
                lst[i] = value

    def sprinkle_nones(self, lst, prob):
        """
        Sprinkle None entries in list *lst* with likelihood *prob*.
        """
        self.sprinkle(lst, prob, None)

    def generate_int_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python ints with *none_prob* probability of
        an entry being None.
        """
        data = list(range(n))
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_float_list(self, n, none_prob=DEFAULT_NONE_PROB,
                            use_nan=False):
        """
        Generate a list of Python floats with *none_prob* probability of
        an entry being None (or NaN if *use_nan* is true).
        """
        # Make sure we get Python floats, not np.float64
        data = list(map(float, self.rnd.uniform(0.0, 1.0, n)))
        assert len(data) == n
        self.sprinkle(data, none_prob, value=float('nan') if use_nan else None)
        return data

    def generate_bool_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of Python bools with *none_prob* probability of
        an entry being None.
        """
        # Make sure we get Python bools, not np.bool_
        data = [bool(x >= 0.5) for x in self.rnd.uniform(0.0, 1.0, n)]
        assert len(data) == n
        self.sprinkle_nones(data, none_prob)
        return data

    def generate_decimal_list(self, n, none_prob=DEFAULT_NONE_PROB,
                              use_nan=False):
        """
        Generate a list of Python Decimals with *none_prob* probability of
        an entry being None (or NaN if *use_nan* is true).
        """
        data = [decimal.Decimal('%.9f' % f)
                for f in self.rnd.uniform(0.0, 1.0, n)]
        assert len(data) == n
        self.sprinkle(data, none_prob,
                      value=decimal.Decimal('nan') if use_nan else None)
        return data

    def generate_object_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of generic Python objects with *none_prob*
        probability of an entry being None.
        """
        data = [object() for i in range(n)]
        self.sprinkle_nones(data, none_prob)
        return data

    def _generate_varying_sequences(self, random_factory, n, min_size,
                                    max_size, none_prob):
        """
        Generate a list of *n* sequences of varying size between *min_size*
        and *max_size*, with *none_prob* probability of an entry being None.
        The base material for each sequence is obtained by calling
        `random_factory(<some size>)`
        """
        base_size = 10000
        base = random_factory(base_size + max_size)
        data = []
        for i in range(n):
            off = self.rnd.randint(base_size)
            if min_size == max_size:
                size = min_size
            else:
                size = self.rnd.randint(min_size, max_size + 1)
            data.append(base[off:off + size])
        self.sprinkle_nones(data, none_prob)
        assert len(data) == n
        return data

    def generate_fixed_binary_list(self, n, size, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a fixed *size*.
        """
        return self._generate_varying_sequences(get_random_bytes, n,
                                                size, size, none_prob)

    def generate_varying_binary_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of bytestrings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_bytes, n,
                                                min_size, max_size, none_prob)

    def generate_ascii_string_list(self, n, min_size, max_size,
                                   none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of ASCII strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_ascii, n,
                                                min_size, max_size, none_prob)

    def generate_unicode_string_list(self, n, min_size, max_size,
                                     none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of unicode strings with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(get_random_unicode, n,
                                                min_size, max_size, none_prob)

    def generate_int_list_list(self, n, min_size, max_size,
                               none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of lists of Python ints with a random size between
        *min_size* and *max_size*.
        """
        return self._generate_varying_sequences(
            partial(self.generate_int_list, none_prob=none_prob),
            n, min_size, max_size, none_prob)

    def generate_tuple_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of tuples with random values.
        Each tuple has the form `(int value, float value, bool value)`
        """
        dicts = self.generate_dict_list(n, none_prob=none_prob)
        tuples = [(d.get('u'), d.get('v'), d.get('w'))
                  if d is not None else None
                  for d in dicts]
        assert len(tuples) == n
        return tuples

    def generate_dict_list(self, n, none_prob=DEFAULT_NONE_PROB):
        """
        Generate a list of dicts with random values.
        Each dict has the form

            `{'u': int value, 'v': float value, 'w': bool value}`
        """
        ints = self.generate_int_list(n, none_prob=none_prob)
        floats = self.generate_float_list(n, none_prob=none_prob)
        bools = self.generate_bool_list(n, none_prob=none_prob)
        dicts = []
        # Keep half the Nones, omit the other half
        keep_nones = itertools.cycle([True, False])
        for u, v, w in zip(ints, floats, bools):
            d = {}
            if u is not None or next(keep_nones):
                d['u'] = u
            if v is not None or next(keep_nones):
                d['v'] = v
            if w is not None or next(keep_nones):
                d['w'] = w
            dicts.append(d)
        self.sprinkle_nones(dicts, none_prob)
        assert len(dicts) == n
        return dicts

    def get_type_and_builtins(self, n, type_name):
        """
        Return a `(arrow type, list)` tuple where the arrow type
        corresponds to the given logical *type_name*, and the list
        is a list of *n* random-generated Python objects compatible
        with the arrow type.
        """
        size = None

        if type_name in ('bool', 'decimal', 'ascii', 'unicode', 'int64 list'):
            kind = type_name
        elif type_name.startswith(('int', 'uint')):
            kind = 'int'
        elif type_name.startswith('float'):
            kind = 'float'
        elif type_name.startswith('struct'):
            kind = 'struct'
        elif type_name == 'binary':
            kind = 'varying binary'
        elif type_name.startswith('binary'):
            kind = 'fixed binary'
            size = int(type_name[6:])
            assert size > 0
        else:
            raise ValueError("unrecognized type %r" % (type_name,))

        if kind in ('int', 'float'):
            ty = getattr(pa, type_name)()
        elif kind == 'bool':
            ty = pa.bool_()
        elif kind == 'decimal':
            ty = pa.decimal128(9, 9)
        elif kind == 'fixed binary':
            ty = pa.binary(size)
        elif kind == 'varying binary':
            ty = pa.binary()
        elif kind in ('ascii', 'unicode'):
            ty = pa.string()
        elif kind == 'int64 list':
            ty = pa.list_(pa.int64())
        elif kind == 'struct':
            ty = pa.struct([pa.field('u', pa.int64()),
                            pa.field('v', pa.float64()),
                            pa.field('w', pa.bool_())])

        factories = {
            'int': self.generate_int_list,
            'float': self.generate_float_list,
            'bool': self.generate_bool_list,
            'decimal': self.generate_decimal_list,
            'fixed binary': partial(self.generate_fixed_binary_list,
                                    size=size),
            'varying binary': partial(self.generate_varying_binary_list,
                                      min_size=3, max_size=40),
            'ascii': partial(self.generate_ascii_string_list,
                             min_size=3, max_size=40),
            'unicode': partial(self.generate_unicode_string_list,
                               min_size=3, max_size=40),
            'int64 list': partial(self.generate_int_list_list,
                                  min_size=0, max_size=20),
            'struct': self.generate_dict_list,
            'struct from tuples': self.generate_tuple_list,
        }
        data = factories[kind](n)
        return ty, data
