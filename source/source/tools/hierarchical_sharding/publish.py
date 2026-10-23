"""Publish a complete snapshot from independently completed shard manifests."""
import argparse
import json
from pathlib import Path
from .transport import publish_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard', action='append', required=True, help='Manifest path followed by =http://host:port')
    parser.add_argument('--output', required=True)
    parser.add_argument('--total-vector-count', type=int, required=True)
    args = parser.parse_args()
    shards = []
    for specification in args.shard:
        path, endpoint = specification.rsplit('=', 1)
        shard = json.loads(Path(path).read_text())
        shard['endpoint'] = endpoint
        shards.append(shard)
    first = shards[0]
    manifest = {key: first[key] for key in ('generation', 'dimension', 'metric')}
    manifest['shards'] = shards
    manifest['total_vector_count'] = args.total_vector_count
    publish_manifest(args.output, manifest)


if __name__ == '__main__':
    main()
