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


def test_device_memory_manager():
    mm = pa.default_cpu_memory_manager()
    assert mm.is_cpu
    device = mm.device
    assert device.is_cpu
    assert device.device_id == -1
    assert device.device_type == pa.DeviceAllocationType.CPU
    assert device.type_name == "arrow::CPUDevice"
    assert device == device
    assert repr(device) == "<pyarrow.Device: CPUDevice()>"
    assert repr(mm) == "<pyarrow.MemoryManager device: CPUDevice()>"


def test_buffer_device():
    arr = pa.array([0, 1, 2])
    buf = arr.buffers()[1]
    assert buf.device_type == pa.DeviceAllocationType.CPU
    assert isinstance(buf.device, pa.Device)
    assert isinstance(buf.memory_manager, pa.MemoryManager)
    assert buf.is_cpu
    assert buf.device.is_cpu
    assert buf.device == pa.default_cpu_memory_manager().device
    assert buf.memory_manager.is_cpu
