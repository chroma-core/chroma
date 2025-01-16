# Copyright 2020 The HuggingFace Datasets Authors and the TensorFlow Datasets Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__version__ = "3.0.0"

from .arrow_dataset import Dataset
from .arrow_reader import ReadInstruction
from .builder import ArrowBasedBuilder, BuilderConfig, DatasetBuilder, GeneratorBasedBuilder
from .combine import concatenate_datasets, interleave_datasets
from .dataset_dict import DatasetDict, IterableDatasetDict
from .download import *
from .features import *
from .fingerprint import disable_caching, enable_caching, is_caching_enabled
from .info import DatasetInfo
from .inspect import (
    get_dataset_config_info,
    get_dataset_config_names,
    get_dataset_default_config_name,
    get_dataset_infos,
    get_dataset_split_names,
)
from .iterable_dataset import IterableDataset
from .load import load_dataset, load_dataset_builder, load_from_disk
from .splits import (
    NamedSplit,
    NamedSplitAll,
    Split,
    SplitBase,
    SplitDict,
    SplitGenerator,
    SplitInfo,
    SubSplitInfo,
    percent,
)
from .utils import *
from .utils import logging
