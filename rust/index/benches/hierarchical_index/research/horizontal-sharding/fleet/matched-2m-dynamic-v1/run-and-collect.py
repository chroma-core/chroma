"""Run the matched driver and collect metadata on return, without cloud actions.

Independent machine shutdown remains required. Nested stage supervisors enforce
work deadlines; this wrapper does not promise to stop detached descendants.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time


def checked(item):
    path = Path(item['path']).resolve(strict=True)
    data = path.read_bytes()
    if len(data) != item['bytes'] or hashlib.sha256(data).hexdigest() != item['sha256']:
        raise ValueError('changed admitted artifact: '+str(path))
    return path


def run(spec):
    base_path = checked(spec['base'])
    driver = checked(spec['driver'])
    collector = checked(spec['collector'])
    collection_path = checked(spec['collection'])
    base = json.loads(base_path.read_text())
    collection = json.loads(collection_path.read_text())
    if not time.time() < base['deadline_unix'] <= collection['deadline_unix'] - 120:
        raise ValueError('reserve at least 120 seconds for collection')
    if collection['deadline_unix'] > spec['machine_stop_unix'] - 60:
        raise ValueError('reserve at least 60 seconds for retrieval before fixed stop')
    restore = spec.get('restore')
    if restore:
        restore_script = checked(restore['script'])
        restore_spec = checked(restore['spec'])
        if json.loads(restore_spec.read_text())['deadline_unix'] != base['deadline_unix']:
            raise ValueError('restore and work deadlines must match')
    output = Path(spec['output'])
    output.mkdir(parents=True, exist_ok=False)
    result = {'work': None, 'collection': None, 'machine_stop_unix': spec['machine_stop_unix']}
    try:
        if restore:
            with (output/'restore.stdout').open('xb') as out, (output/'restore.stderr').open('xb') as err:
                result['restore'] = subprocess.run([sys.executable, str(restore_script), '--spec', str(restore_spec)], stdout=out, stderr=err).returncode
            if result['restore'] != 0:
                raise RuntimeError('restore failed; matched driver skipped')
        with (output/'work.stdout').open('xb') as out, (output/'work.stderr').open('xb') as err:
            result['work'] = subprocess.run([sys.executable, str(driver), '--base', str(base_path)], stdout=out, stderr=err).returncode
    except Exception as exc:
        result['work_error'] = repr(exc)
    finally:
        remaining = collection['deadline_unix'] - time.time()
        if remaining <= 0:
            result['collection_error'] = 'collection deadline exhausted; fixed stop unchanged'
        else:
            try:
                checked(spec['collector'])
                checked(spec['collection'])
                with (output/'collection.stdout').open('xb') as out, (output/'collection.stderr').open('xb') as err:
                    result['collection'] = subprocess.run(
                        [sys.executable, str(collector), '--spec', str(collection_path), '--output', str(output/'evidence.tar')],
                        stdout=out, stderr=err, timeout=remaining).returncode
            except Exception as exc:
                result['collection_error'] = repr(exc)
        result['ended_unix'] = time.time()
        (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    result = run(json.loads(Path(args.spec).read_text()))
    sys.exit(0 if result['work'] == result['collection'] == 0 else 1)
