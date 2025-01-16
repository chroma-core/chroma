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

import numpy as np
import pandas as pd
import pyarrow as pa

from . import common
from .common import KILOBYTE, MEGABYTE


def generate_chunks(total_size, nchunks, ncols, dtype=np.dtype('int64')):
    rowsize = total_size // nchunks // ncols
    assert rowsize % dtype.itemsize == 0

    def make_column(col, chunk):
        return np.frombuffer(common.get_random_bytes(
            rowsize, seed=col + 997 * chunk)).view(dtype)

    return [pd.DataFrame({
            'c' + str(col): make_column(col, chunk)
            for col in range(ncols)})
            for chunk in range(nchunks)]


class StreamReader(object):
    """
    Benchmark in-memory streaming to a Pandas dataframe.
    """
    total_size = 64 * MEGABYTE
    ncols = 8
    chunk_sizes = [16 * KILOBYTE, 256 * KILOBYTE, 8 * MEGABYTE]

    param_names = ['chunk_size']
    params = [chunk_sizes]

    def setup(self, chunk_size):
        # Note we're careful to stream different chunks instead of
        # streaming N times the same chunk, so that we avoid operating
        # entirely out of L1/L2.
        chunks = generate_chunks(self.total_size,
                                 nchunks=self.total_size // chunk_size,
                                 ncols=self.ncols)
        batches = [pa.RecordBatch.from_pandas(df)
                   for df in chunks]
        schema = batches[0].schema
        sink = pa.BufferOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(sink, schema)
        for batch in batches:
            stream_writer.write_batch(batch)
        self.source = sink.getvalue()

    def time_read_to_dataframe(self, *args):
        reader = pa.RecordBatchStreamReader(self.source)
        table = reader.read_all()
        df = table.to_pandas()  # noqa
