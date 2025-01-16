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

import time
import pyarrow as pa


class HighLatencyReader(object):

    def __init__(self, raw, latency):
        self.raw = raw
        self.latency = latency

    def close(self):
        self.raw.close()

    @property
    def closed(self):
        return self.raw.closed

    def read(self, nbytes=None):
        time.sleep(self.latency)
        return self.raw.read(nbytes)


class HighLatencyWriter(object):

    def __init__(self, raw, latency):
        self.raw = raw
        self.latency = latency

    def close(self):
        self.raw.close()

    @property
    def closed(self):
        return self.raw.closed

    def write(self, data):
        time.sleep(self.latency)
        self.raw.write(data)


class BufferedIOHighLatency(object):
    """Benchmark creating a parquet manifest."""

    increment = 1024
    total_size = 16 * (1 << 20)  # 16 MB
    buffer_size = 1 << 20  # 1 MB
    latency = 0.1  # 100ms

    param_names = ('latency',)
    params = [0, 0.01, 0.1]

    def time_buffered_writes(self, latency):
        test_data = b'x' * self.increment
        bytes_written = 0
        out = pa.BufferOutputStream()
        slow_out = HighLatencyWriter(out, latency)
        buffered_out = pa.output_stream(slow_out, buffer_size=self.buffer_size)

        while bytes_written < self.total_size:
            buffered_out.write(test_data)
            bytes_written += self.increment
        buffered_out.flush()

    def time_buffered_reads(self, latency):
        bytes_read = 0
        reader = pa.input_stream(pa.py_buffer(b'x' * self.total_size))
        slow_reader = HighLatencyReader(reader, latency)
        buffered_reader = pa.input_stream(slow_reader,
                                          buffer_size=self.buffer_size)
        while bytes_read < self.total_size:
            buffered_reader.read(self.increment)
            bytes_read += self.increment
