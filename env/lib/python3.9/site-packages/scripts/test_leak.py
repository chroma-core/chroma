#!/usr/bin/env python

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
import numpy as np
import pandas as pd
from pyarrow.tests.util import rands
import memory_profiler
import gc
import io

MEGABYTE = 1 << 20


def assert_does_not_leak(f, iterations=10, check_interval=1, tolerance=5):
    gc.collect()
    baseline = memory_profiler.memory_usage()[0]
    for i in range(iterations):
        f()
        if i % check_interval == 0:
            gc.collect()
            usage = memory_profiler.memory_usage()[0]
            diff = usage - baseline
            print("{0}: {1}\r".format(i, diff), end="")
            if diff > tolerance:
                raise Exception("Memory increased by {0} megabytes after {1} "
                                "iterations".format(diff, i + 1))
    gc.collect()
    usage = memory_profiler.memory_usage()[0]
    diff = usage - baseline
    print("\nMemory increased by {0} megabytes after {1} "
          "iterations".format(diff, iterations))


def test_leak1():
    data = [pa.array(np.concatenate([np.random.randn(100000)] * 1000))]
    table = pa.Table.from_arrays(data, ['foo'])

    def func():
        table.to_pandas()
    assert_does_not_leak(func)


def test_leak2():
    data = [pa.array(np.concatenate([np.random.randn(100000)] * 10))]
    table = pa.Table.from_arrays(data, ['foo'])

    def func():
        df = table.to_pandas()

        batch = pa.RecordBatch.from_pandas(df)

        sink = io.BytesIO()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()

        buf_reader = pa.BufferReader(sink.getvalue())
        reader = pa.open_file(buf_reader)
        reader.read_all()

    assert_does_not_leak(func, iterations=50, tolerance=50)


def test_leak3():
    import pyarrow.parquet as pq

    df = pd.DataFrame({'a{0}'.format(i): [1, 2, 3, 4]
                       for i in range(50)})
    table = pa.Table.from_pandas(df, preserve_index=False)

    writer = pq.ParquetWriter('leak_test_' + rands(5) + '.parquet',
                              table.schema)

    def func():
        writer.write_table(table, row_group_size=len(table))

    # This does not "leak" per se but we do want to have this use as little
    # memory as possible
    assert_does_not_leak(func, iterations=500,
                         check_interval=50, tolerance=20)


def test_ARROW_8801():
    x = pd.to_datetime(np.random.randint(0, 2**32, size=2**20, dtype=np.int64),
                       unit='ms', utc=True)
    table = pa.table(pd.DataFrame({'x': x}))

    assert_does_not_leak(lambda: table.to_pandas(split_blocks=False),
                         iterations=1000, check_interval=50, tolerance=1000)


if __name__ == '__main__':
    test_ARROW_8801()
