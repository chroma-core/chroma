"""Build the frozen native source inside a separately admitted Linux window."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    if platform.system() != 'Linux':
        raise RuntimeError('Linux test machine required')
    spec = json.loads(Path(args.spec).read_text())
    sys.path.insert(0, spec['runner_directory'])
    from artifact_bindings import descriptor, verify_manifest, verify_descriptor
    from supervise import run
    verify_manifest(spec['runner_artifacts'])
    verify_manifest(spec['source_manifest'])
    compiler = verify_descriptor(spec['compiler'])
    cargo = verify_descriptor(spec['cargo'])
    forbidden = [key for key in os.environ if key in ('RUSTC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER',
                 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_RUSTFLAGS', 'CARGO_BUILD_TARGET') or key.startswith('CARGO_TARGET_')]
    if forbidden:
        raise ValueError('ambient compiler overrides: ' + ','.join(forbidden))
    subprocess.run(['systemctl', 'is-active', '--quiet', spec['guard_timer']], check=True)
    output = Path(spec['output'])
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, RUSTC=str(compiler), CARGO_TARGET_DIR=spec['target_directory'],
               CARGO_PROFILE_RELEASE_DEBUG='0', CARGO_INCREMENTAL='0')
    command = [str(cargo), 'build', '--release', '--locked', '--offline', '-j2',
               '-p', 'chroma-index', '--bench', 'hierarchical_spann_shard', '--message-format=json']
    run(command, spec['source_directory'], output/'process', spec['deadline_unix'],
        spec['min_free_bytes'], spec['max_process_rss_bytes'], spec['max_log_bytes'], env)
    artifacts = []
    for line in (output/'process/stdout').read_text().splitlines():
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('target', {}).get('name') == 'hierarchical_spann_shard' and item.get('executable'):
            artifacts.append(Path(item['executable']))
    if len(artifacts) != 1:
        raise ValueError('expected exactly one native compiler artifact')
    shutil.copyfile(artifacts[0], output/'native')
    (output/'native').chmod(0o755)
    verify_manifest(spec['source_manifest'])
    verify_manifest(spec['runner_artifacts'])
    provenance = dict(source_manifest=spec['source_manifest'], compiler=spec['compiler'],
                      compiler_version=subprocess.check_output([str(compiler), '--version', '--verbose'], text=True),
                      cargo=spec['cargo'], binary=descriptor(output/'native'), build_command=command,
                      target_directory=spec['target_directory'], architecture=platform.machine(),
                      build_spec=descriptor(args.spec), process_receipt=descriptor(output/'process/result.json'))
    with (output/'provenance.json').open('x') as stream:
        json.dump(provenance, stream, indent=2)
        stream.write('\n')


if __name__ == '__main__':
    main()
