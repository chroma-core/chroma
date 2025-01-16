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

import pyarrow.benchmark as pb

from . import common


class PandasObjectIsNull(object):
    size = 10 ** 5
    types = ('int', 'float', 'object', 'decimal')

    param_names = ['type']
    params = [types]

    def setup(self, type_name):
        gen = common.BuiltinsGenerator()
        if type_name == 'int':
            lst = gen.generate_int_list(self.size)
        elif type_name == 'float':
            lst = gen.generate_float_list(self.size, use_nan=True)
        elif type_name == 'object':
            lst = gen.generate_object_list(self.size)
        elif type_name == 'decimal':
            lst = gen.generate_decimal_list(self.size)
        else:
            assert 0
        self.lst = lst

    def time_PandasObjectIsNull(self, *args):
        pb.benchmark_PandasObjectIsNull(self.lst)
