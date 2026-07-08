import os

import yaml

from chromadb.cli.utils import (
    get_directory_size,
    set_log_file_path,
    sizeof_fmt,
)


def test_sizeof_fmt_small_values_stay_in_bytes():
    assert sizeof_fmt(0) == "0.0B"
    assert sizeof_fmt(512) == "512.0B"


def test_sizeof_fmt_scales_through_units():
    assert sizeof_fmt(1024) == "1.0KiB"
    assert sizeof_fmt(1536) == "1.5KiB"
    assert sizeof_fmt(1024**2) == "1.0MiB"
    assert sizeof_fmt(1024**3 + 1024**3 // 2) == "1.5GiB"


def test_sizeof_fmt_honors_custom_suffix():
    assert sizeof_fmt(1024, suffix="") == "1.0Ki"


def test_sizeof_fmt_beyond_zebi_falls_back_to_yobi():
    # Values past the unit table are reported in yobibytes.
    assert sizeof_fmt(1024**9).endswith("YiB")


def test_get_directory_size_sums_files_recursively(tmp_path):
    (tmp_path / "a.txt").write_bytes(b"12345")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.txt").write_bytes(b"678")
    assert get_directory_size(str(tmp_path)) == 8


def test_get_directory_size_empty_directory(tmp_path):
    assert get_directory_size(str(tmp_path)) == 0


def test_set_log_file_path_updates_rotating_file_handler(tmp_path):
    config = {
        "handlers": {
            "console": {"class": "logging.StreamHandler"},
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "old.log",
            },
        }
    }
    config_path = tmp_path / "log_config.yml"
    config_path.write_text(yaml.safe_dump(config))

    updated = set_log_file_path(str(config_path), new_filename="new.log")

    # Only the rotating file handler is repointed; others are untouched.
    assert updated["handlers"]["file"]["filename"] == "new.log"
    assert "filename" not in updated["handlers"]["console"]


def test_set_log_file_path_defaults_to_chroma_log(tmp_path):
    config = {
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": "old.log",
            }
        }
    }
    config_path = tmp_path / "log_config.yml"
    config_path.write_text(yaml.safe_dump(config))

    updated = set_log_file_path(str(config_path))
    assert updated["handlers"]["file"]["filename"] == "chroma.log"
    # sanity: the file we wrote is what was read back
    assert os.path.exists(config_path)
