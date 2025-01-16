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


class ScalarAccess(object):
    n = 10 ** 5

    def setUp(self):
        self._array = pa.array(list(range(self.n)), type=pa.int64())
        self._array_items = list(self._array)

    def time_getitem(self):
        for i in range(self.n):
            self._array[i]

    def time_as_py(self):
        for item in self._array_items:
            item.as_py()
