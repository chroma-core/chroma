import sys

import chromadb_rust_bindings


def build_cli_args(**kwargs):
    args = []
    for key, value in kwargs.items():
        if isinstance(value, bool):
            if value:
                args.append(f"--{key}")
        elif value is not None:
            args.extend([f"--{key}", str(value)])
    return args


def app():
    try:
        args = sys.argv
        chromadb_rust_bindings.cli(args)
    except KeyboardInterrupt:
        pass