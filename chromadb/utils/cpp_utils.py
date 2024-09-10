import sys
import os
import ctypes

CHROMA_CPP_LIB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "lib"
)


def load_cpp_lib(lib_name: str) -> ctypes.CDLL:
    # Determine the extension of the shared library based on the platform
    if sys.platform == "darwin":
        ext = "dylib"
    elif sys.platform == "win32":
        ext = "dll"
    else:
        ext = "so"

    lib_path = os.path.join(CHROMA_CPP_LIB_PATH, f"lib{lib_name}.{ext}")
    return ctypes.CDLL(lib_path)
