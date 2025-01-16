# Copyright 2020 The HuggingFace Datasets Authors.
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

# Lint as: python3
"""List and inspect datasets."""

import os
from typing import Dict, List, Mapping, Optional, Sequence, Union

from .download.download_config import DownloadConfig
from .download.download_manager import DownloadMode
from .download.streaming_download_manager import StreamingDownloadManager
from .info import DatasetInfo
from .load import (
    dataset_module_factory,
    get_dataset_builder_class,
    load_dataset_builder,
)
from .utils.logging import get_logger
from .utils.version import Version


logger = get_logger(__name__)


class SplitsNotFoundError(ValueError):
    pass


def get_dataset_infos(
    path: str,
    data_files: Optional[Union[Dict, List, str]] = None,
    download_config: Optional[DownloadConfig] = None,
    download_mode: Optional[Union[DownloadMode, str]] = None,
    revision: Optional[Union[str, Version]] = None,
    token: Optional[Union[bool, str]] = None,
    **config_kwargs,
):
    """Get the meta information about a dataset, returned as a dict mapping config name to DatasetInfoDict.

    Args:
        path (`str`): path to the dataset processing script with the dataset builder. Can be either:

            - a local path to processing script or the directory containing the script (if the script has the same name as the directory),
                e.g. `'./dataset/squad'` or `'./dataset/squad/squad.py'`
            - a dataset identifier on the Hugging Face Hub (list all available datasets and ids with [`huggingface_hub.list_datasets`]),
                e.g. `'squad'`, `'glue'` or``'openai/webtext'`
        revision (`Union[str, datasets.Version]`, *optional*):
            If specified, the dataset module will be loaded from the datasets repository at this version.
            By default:
            - it is set to the local version of the lib.
            - it will also try to load it from the main branch if it's not available at the local version of the lib.
            Specifying a version that is different from your local version of the lib might cause compatibility issues.
        download_config ([`DownloadConfig`], *optional*):
            Specific download configuration parameters.
        download_mode ([`DownloadMode`] or `str`, defaults to `REUSE_DATASET_IF_EXISTS`):
            Download/generate mode.
        data_files (`Union[Dict, List, str]`, *optional*):
            Defining the data_files of the dataset configuration.
        token (`str` or `bool`, *optional*):
            Optional string or boolean to use as Bearer token for remote files on the Datasets Hub.
            If `True`, or not specified, will get token from `"~/.huggingface"`.
        **config_kwargs (additional keyword arguments):
            Optional attributes for builder class which will override the attributes if supplied.

    Example:

    ```py
    >>> from datasets import get_dataset_infos
    >>> get_dataset_infos('rotten_tomatoes')
    {'default': DatasetInfo(description="Movie Review Dataset.\nThis is a dataset of containing 5,331 positive and 5,331 negative processed\nsentences from Rotten Tomatoes movie reviews...), ...}
    ```
    """
    config_names = get_dataset_config_names(
        path=path,
        revision=revision,
        download_config=download_config,
        download_mode=download_mode,
        data_files=data_files,
        token=token,
    )
    return {
        config_name: get_dataset_config_info(
            path=path,
            config_name=config_name,
            data_files=data_files,
            download_config=download_config,
            download_mode=download_mode,
            revision=revision,
            token=token,
            **config_kwargs,
        )
        for config_name in config_names
    }


def get_dataset_config_names(
    path: str,
    revision: Optional[Union[str, Version]] = None,
    download_config: Optional[DownloadConfig] = None,
    download_mode: Optional[Union[DownloadMode, str]] = None,
    dynamic_modules_path: Optional[str] = None,
    data_files: Optional[Union[Dict, List, str]] = None,
    **download_kwargs,
):
    """Get the list of available config names for a particular dataset.

    Args:
        path (`str`): path to the dataset processing script with the dataset builder. Can be either:

            - a local path to processing script or the directory containing the script (if the script has the same name as the directory),
                e.g. `'./dataset/squad'` or `'./dataset/squad/squad.py'`
            - a dataset identifier on the Hugging Face Hub (list all available datasets and ids with [`huggingface_hub.list_datasets`]),
                e.g. `'squad'`, `'glue'` or `'openai/webtext'`
        revision (`Union[str, datasets.Version]`, *optional*):
            If specified, the dataset module will be loaded from the datasets repository at this version.
            By default:
            - it is set to the local version of the lib.
            - it will also try to load it from the main branch if it's not available at the local version of the lib.
            Specifying a version that is different from your local version of the lib might cause compatibility issues.
        download_config ([`DownloadConfig`], *optional*):
            Specific download configuration parameters.
        download_mode ([`DownloadMode`] or `str`, defaults to `REUSE_DATASET_IF_EXISTS`):
            Download/generate mode.
        dynamic_modules_path (`str`, defaults to `~/.cache/huggingface/modules/datasets_modules`):
            Optional path to the directory in which the dynamic modules are saved. It must have been initialized with `init_dynamic_modules`.
            By default the datasets are stored inside the `datasets_modules` module.
        data_files (`Union[Dict, List, str]`, *optional*):
            Defining the data_files of the dataset configuration.
        **download_kwargs (additional keyword arguments):
            Optional attributes for [`DownloadConfig`] which will override the attributes in `download_config` if supplied,
            for example `token`.

    Example:

    ```py
    >>> from datasets import get_dataset_config_names
    >>> get_dataset_config_names("glue")
    ['cola',
     'sst2',
     'mrpc',
     'qqp',
     'stsb',
     'mnli',
     'mnli_mismatched',
     'mnli_matched',
     'qnli',
     'rte',
     'wnli',
     'ax']
    ```
    """
    dataset_module = dataset_module_factory(
        path,
        revision=revision,
        download_config=download_config,
        download_mode=download_mode,
        dynamic_modules_path=dynamic_modules_path,
        data_files=data_files,
        **download_kwargs,
    )
    builder_cls = get_dataset_builder_class(dataset_module, dataset_name=os.path.basename(path))
    return list(builder_cls.builder_configs.keys()) or [
        dataset_module.builder_kwargs.get("config_name", builder_cls.DEFAULT_CONFIG_NAME or "default")
    ]


def get_dataset_default_config_name(
    path: str,
    revision: Optional[Union[str, Version]] = None,
    download_config: Optional[DownloadConfig] = None,
    download_mode: Optional[Union[DownloadMode, str]] = None,
    dynamic_modules_path: Optional[str] = None,
    data_files: Optional[Union[Dict, List, str]] = None,
    **download_kwargs,
) -> Optional[str]:
    """Get the default config name for a particular dataset.
    Can return None only if the dataset has multiple configurations and no default configuration.

    Args:
        path (`str`): path to the dataset processing script with the dataset builder. Can be either:

            - a local path to processing script or the directory containing the script (if the script has the same name as the directory),
                e.g. `'./dataset/squad'` or `'./dataset/squad/squad.py'`
            - a dataset identifier on the Hugging Face Hub (list all available datasets and ids with [`huggingface_hub.list_datasets`]),
                e.g. `'squad'`, `'glue'` or `'openai/webtext'`
        revision (`Union[str, datasets.Version]`, *optional*):
            If specified, the dataset module will be loaded from the datasets repository at this version.
            By default:
            - it is set to the local version of the lib.
            - it will also try to load it from the main branch if it's not available at the local version of the lib.
            Specifying a version that is different from your local version of the lib might cause compatibility issues.
        download_config ([`DownloadConfig`], *optional*):
            Specific download configuration parameters.
        download_mode ([`DownloadMode`] or `str`, defaults to `REUSE_DATASET_IF_EXISTS`):
            Download/generate mode.
        dynamic_modules_path (`str`, defaults to `~/.cache/huggingface/modules/datasets_modules`):
            Optional path to the directory in which the dynamic modules are saved. It must have been initialized with `init_dynamic_modules`.
            By default the datasets are stored inside the `datasets_modules` module.
        data_files (`Union[Dict, List, str]`, *optional*):
            Defining the data_files of the dataset configuration.
        **download_kwargs (additional keyword arguments):
            Optional attributes for [`DownloadConfig`] which will override the attributes in `download_config` if supplied,
            for example `token`.

    Returns:
        Optional[str]: the default config name if there is one

    Example:

    ```py
    >>> from datasets import get_dataset_default_config_name
    >>> get_dataset_default_config_name("openbookqa")
    'main'
    ```
    """
    dataset_module = dataset_module_factory(
        path,
        revision=revision,
        download_config=download_config,
        download_mode=download_mode,
        dynamic_modules_path=dynamic_modules_path,
        data_files=data_files,
        **download_kwargs,
    )
    builder_cls = get_dataset_builder_class(dataset_module, dataset_name=os.path.basename(path))
    builder_configs = list(builder_cls.builder_configs.keys())
    if builder_configs:
        default_config_name = builder_configs[0] if len(builder_configs) == 1 else None
    else:
        default_config_name = "default"
    return builder_cls.DEFAULT_CONFIG_NAME or default_config_name


def get_dataset_config_info(
    path: str,
    config_name: Optional[str] = None,
    data_files: Optional[Union[str, Sequence[str], Mapping[str, Union[str, Sequence[str]]]]] = None,
    download_config: Optional[DownloadConfig] = None,
    download_mode: Optional[Union[DownloadMode, str]] = None,
    revision: Optional[Union[str, Version]] = None,
    token: Optional[Union[bool, str]] = None,
    **config_kwargs,
) -> DatasetInfo:
    """Get the meta information (DatasetInfo) about a dataset for a particular config

    Args:
        path (``str``): path to the dataset processing script with the dataset builder. Can be either:

            - a local path to processing script or the directory containing the script (if the script has the same name as the directory),
                e.g. ``'./dataset/squad'`` or ``'./dataset/squad/squad.py'``
            - a dataset identifier on the Hugging Face Hub (list all available datasets and ids with [`huggingface_hub.list_datasets`]),
                e.g. ``'squad'``, ``'glue'`` or ``'openai/webtext'``
        config_name (:obj:`str`, optional): Defining the name of the dataset configuration.
        data_files (:obj:`str` or :obj:`Sequence` or :obj:`Mapping`, optional): Path(s) to source data file(s).
        download_config (:class:`~download.DownloadConfig`, optional): Specific download configuration parameters.
        download_mode (:class:`DownloadMode` or :obj:`str`, default ``REUSE_DATASET_IF_EXISTS``): Download/generate mode.
        revision (:class:`~utils.Version` or :obj:`str`, optional): Version of the dataset script to load.
            As datasets have their own git repository on the Datasets Hub, the default version "main" corresponds to their "main" branch.
            You can specify a different version than the default "main" by using a commit SHA or a git tag of the dataset repository.
        token (``str`` or :obj:`bool`, optional): Optional string or boolean to use as Bearer token for remote files on the Datasets Hub.
            If True, or not specified, will get token from `"~/.huggingface"`.
        **config_kwargs (additional keyword arguments): optional attributes for builder class which will override the attributes if supplied.

    """
    builder = load_dataset_builder(
        path,
        name=config_name,
        data_files=data_files,
        download_config=download_config,
        download_mode=download_mode,
        revision=revision,
        token=token,
        **config_kwargs,
    )
    info = builder.info
    if info.splits is None:
        download_config = download_config.copy() if download_config else DownloadConfig()
        if token is not None:
            download_config.token = token
        builder._check_manual_download(
            StreamingDownloadManager(base_path=builder.base_path, download_config=download_config)
        )
        try:
            info.splits = {
                split_generator.name: {"name": split_generator.name, "dataset_name": path}
                for split_generator in builder._split_generators(
                    StreamingDownloadManager(base_path=builder.base_path, download_config=download_config)
                )
            }
        except Exception as err:
            raise SplitsNotFoundError("The split names could not be parsed from the dataset config.") from err
    return info


def get_dataset_split_names(
    path: str,
    config_name: Optional[str] = None,
    data_files: Optional[Union[str, Sequence[str], Mapping[str, Union[str, Sequence[str]]]]] = None,
    download_config: Optional[DownloadConfig] = None,
    download_mode: Optional[Union[DownloadMode, str]] = None,
    revision: Optional[Union[str, Version]] = None,
    token: Optional[Union[bool, str]] = None,
    **config_kwargs,
):
    """Get the list of available splits for a particular config and dataset.

    Args:
        path (`str`): path to the dataset processing script with the dataset builder. Can be either:

            - a local path to processing script or the directory containing the script (if the script has the same name as the directory),
                e.g. `'./dataset/squad'` or `'./dataset/squad/squad.py'`
            - a dataset identifier on the Hugging Face Hub (list all available datasets and ids with [`huggingface_hub.list_datasets`]),
                e.g. `'squad'`, `'glue'` or `'openai/webtext'`
        config_name (`str`, *optional*):
            Defining the name of the dataset configuration.
        data_files (`str` or `Sequence` or `Mapping`, *optional*):
            Path(s) to source data file(s).
        download_config ([`DownloadConfig`], *optional*):
            Specific download configuration parameters.
        download_mode ([`DownloadMode`] or `str`, defaults to `REUSE_DATASET_IF_EXISTS`):
            Download/generate mode.
        revision ([`Version`] or `str`, *optional*):
            Version of the dataset script to load.
            As datasets have their own git repository on the Datasets Hub, the default version "main" corresponds to their "main" branch.
            You can specify a different version than the default "main" by using a commit SHA or a git tag of the dataset repository.
        token (`str` or `bool`, *optional*):
            Optional string or boolean to use as Bearer token for remote files on the Datasets Hub.
            If `True`, or not specified, will get token from `"~/.huggingface"`.
        **config_kwargs (additional keyword arguments):
            Optional attributes for builder class which will override the attributes if supplied.

    Example:

    ```py
    >>> from datasets import get_dataset_split_names
    >>> get_dataset_split_names('rotten_tomatoes')
    ['train', 'validation', 'test']
    ```
    """
    info = get_dataset_config_info(
        path,
        config_name=config_name,
        data_files=data_files,
        download_config=download_config,
        download_mode=download_mode,
        revision=revision,
        token=token,
        **config_kwargs,
    )
    return list(info.splits.keys())
