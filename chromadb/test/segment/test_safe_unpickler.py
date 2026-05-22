import io
import os
import pickle
import pytest

pytestmark = pytest.mark.skip(
    reason="Python segment persistence types were removed with the Rust backend migration"
)


def test_safe_unpickler_blocks_exploit():
    """Malicious pickle payload must be rejected (CWE-502)"""
    class Exploit:
        def __reduce__(self):
            return (os.system, ("echo pwned",))

    buf = io.BytesIO()
    pickle.dump(Exploit(), buf)
    buf.seek(0)

    with pytest.raises(pickle.UnpicklingError):
        SafeUnpickler(buf).load()


def test_safe_unpickler_loads_valid_data():
    """Valid PersistentData must load correctly in memory"""
    data = PersistentData(
        dimensionality=128,
        total_elements_added=10,
        id_to_label={"abc": 1},
        label_to_id={1: "abc"},
        id_to_seq_id={"abc": 1},
    )

    buf = io.BytesIO()
    pickle.dump(data, buf)
    buf.seek(0)

    result = SafeUnpickler(buf).load()
    assert result.dimensionality == 128
    assert result.total_elements_added == 10
    assert result.id_to_label == {"abc": 1}


def test_load_from_file_backward_compatibility(tmp_path):
    """Test loading a real persisted pickle file from disk - verifies backward compatibility
    with existing serialized indices as requested in issue #6926"""
    data = PersistentData(
        dimensionality=128,
        total_elements_added=10,
        id_to_label={"abc": 1, "def": 2},
        label_to_id={1: "abc", 2: "def"},
        id_to_seq_id={"abc": 1, "def": 2},
    )

    # Save to a real file on disk exactly as ChromaDB would
    filepath = tmp_path / "index_metadata.pickle"
    with open(filepath, "wb") as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)

    # Load using the actual load_from_file method with SafeUnpickler
    result = PersistentData.load_from_file(str(filepath))

    assert result.dimensionality == 128
    assert result.total_elements_added == 10
    assert result.id_to_label == {"abc": 1, "def": 2}
    assert result.label_to_id == {1: "abc", 2: "def"}
    assert result.id_to_seq_id == {"abc": 1, "def": 2}


def test_load_from_file_blocks_malicious_pickle(tmp_path):
    """Malicious pickle file on disk must be rejected by load_from_file"""
    class Exploit:
        def __reduce__(self):
            return (os.system, ("echo pwned",))

    # Write malicious pickle to disk exactly as an attacker would
    filepath = tmp_path / "index_metadata.pickle"
    with open(filepath, "wb") as f:
        pickle.dump(Exploit(), f)

    with pytest.raises(pickle.UnpicklingError):
        PersistentData.load_from_file(str(filepath))
