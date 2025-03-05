import sys
import chromadb_rust_bindings


def main():
    try:
        args = sys.argv
        chromadb_rust_bindings.run_cli(args)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
