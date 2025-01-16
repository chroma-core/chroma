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

import pyarrow as pa

from . import common


# TODO:
# - test dates and times


class ConvertPyListToArray(object):
    """
    Benchmark pa.array(list of values, type=...)
    """
    size = 10 ** 5
    types = ('int32', 'uint32', 'int64', 'uint64',
             'float32', 'float64', 'bool', 'decimal',
             'binary', 'binary10', 'ascii', 'unicode',
             'int64 list', 'struct', 'struct from tuples')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = common.BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)

    def time_convert(self, *args):
        pa.array(self.data, type=self.ty)


class InferPyListToArray(object):
    """
    Benchmark pa.array(list of values) with type inference
    """
    size = 10 ** 5
    types = ('int64', 'float64', 'bool', 'decimal', 'binary', 'ascii',
             'unicode', 'int64 list', 'struct')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = common.BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)

    def time_infer(self, *args):
        arr = pa.array(self.data)
        assert arr.type == self.ty


class ConvertArrayToPyList(object):
    """
    Benchmark pa.array.to_pylist()
    """
    size = 10 ** 5
    types = ('int32', 'uint32', 'int64', 'uint64',
             'float32', 'float64', 'bool', 'decimal',
             'binary', 'binary10', 'ascii', 'unicode',
             'int64 list', 'struct')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = common.BuiltinsGenerator()
        self.ty, self.data = gen.get_type_and_builtins(self.size, type_name)
        self.arr = pa.array(self.data, type=self.ty)

    def time_convert(self, *args):
        self.arr.to_pylist()
