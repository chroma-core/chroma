# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 The HuggingFace Authors.
from typing import Any, Dict, List, Optional, Union

from huggingface_hub import HfFileSystem

from . import config
from .table import CastError
from .utils.track import TrackedIterableFromGenerator, tracked_list, tracked_str


class DatasetsError(Exception):
    """Base class for exceptions in this library."""


class DefunctDatasetError(DatasetsError):
    """The dataset has been defunct."""


class FileNotFoundDatasetsError(DatasetsError, FileNotFoundError):
    """FileNotFoundError raised by this library."""


class DataFilesNotFoundError(FileNotFoundDatasetsError):
    """No (supported) data files found."""


class DatasetNotFoundError(FileNotFoundDatasetsError):
    """Dataset not found.

    Raised when trying to access:
    - a missing dataset, or
    - a private/gated dataset and the user is not authenticated.
    """


class DatasetBuildError(DatasetsError):
    pass


class ManualDownloadError(DatasetBuildError):
    pass


class FileFormatError(DatasetBuildError):
    pass


class DatasetGenerationError(DatasetBuildError):
    pass


class DatasetGenerationCastError(DatasetGenerationError):
    @classmethod
    def from_cast_error(
        cls,
        cast_error: CastError,
        builder_name: str,
        gen_kwargs: Dict[str, Any],
        token: Optional[Union[bool, str]],
    ) -> "DatasetGenerationCastError":
        explanation_message = (
            f"\n\nAll the data files must have the same columns, but at some point {cast_error.details()}"
        )
        formatted_tracked_gen_kwargs: List[str] = []
        for gen_kwarg in gen_kwargs.values():
            if not isinstance(gen_kwarg, (tracked_str, tracked_list, TrackedIterableFromGenerator)):
                continue
            while (
                isinstance(gen_kwarg, (tracked_list, TrackedIterableFromGenerator)) and gen_kwarg.last_item is not None
            ):
                gen_kwarg = gen_kwarg.last_item
            if isinstance(gen_kwarg, tracked_str):
                gen_kwarg = gen_kwarg.get_origin()
            if isinstance(gen_kwarg, str) and gen_kwarg.startswith("hf://"):
                resolved_path = HfFileSystem(endpoint=config.HF_ENDPOINT, token=token).resolve_path(gen_kwarg)
                gen_kwarg = "hf://" + resolved_path.unresolve()
                if "@" + resolved_path.revision in gen_kwarg:
                    gen_kwarg = (
                        gen_kwarg.replace("@" + resolved_path.revision, "", 1)
                        + f" (at revision {resolved_path.revision})"
                    )
            formatted_tracked_gen_kwargs.append(str(gen_kwarg))
        if formatted_tracked_gen_kwargs:
            explanation_message += f"\n\nThis happened while the {builder_name} dataset builder was generating data using\n\n{', '.join(formatted_tracked_gen_kwargs)}"
        help_message = "\n\nPlease either edit the data files to have matching columns, or separate them into different configurations (see docs at https://hf.co/docs/hub/datasets-manual-configuration#multiple-configurations)"
        return cls("An error occurred while generating the dataset" + explanation_message + help_message)


class ChecksumVerificationError(DatasetsError):
    """Error raised during checksums verifications of downloaded files."""


class UnexpectedDownloadedFileError(ChecksumVerificationError):
    """Some downloaded files were not expected."""


class ExpectedMoreDownloadedFilesError(ChecksumVerificationError):
    """Some files were supposed to be downloaded but were not."""


class NonMatchingChecksumError(ChecksumVerificationError):
    """The downloaded file checksum don't match the expected checksum."""


class SplitsVerificationError(DatasetsError):
    """Error raised during splits verifications."""


class UnexpectedSplitsError(SplitsVerificationError):
    """The expected splits of the downloaded file is missing."""


class ExpectedMoreSplitsError(SplitsVerificationError):
    """Some recorded splits are missing."""


class NonMatchingSplitsSizesError(SplitsVerificationError):
    """The splits sizes don't match the expected splits sizes."""
