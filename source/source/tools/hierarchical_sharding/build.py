"""Prepare stable modulo shards and launch independent local or SSH build jobs."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time


def partition(source, output, dimension, shard_count, source_id, start_id=0):
    """Partition a dense corpus suffix without retaining the corpus in memory."""
    if dimension <= 0 or shard_count <= 0 or start_id < 0 or not source_id:
        raise ValueError('positive dimension/shard count and a source identity are required')
    source, output = Path(source), Path(output)
    row_bytes = 4 * dimension
    size = source.stat().st_size
    if size == 0 or size % row_bytes:
        raise ValueError('input must contain complete nonempty float32 rows')
    if size // row_bytes < shard_count:
        raise ValueError('each input batch must contain at least one row per shard')
    output.mkdir(parents=True, exist_ok=False)
    streams, hashes = [], [hashlib.sha256() for _ in range(shard_count)]
    counts = [0] * shard_count
    input_hash = hashlib.sha256()
    started = time.monotonic()
    try:
        streams = [(output / f'shard-{i}.f32').open('xb', buffering=1024 * 1024)
                   for i in range(shard_count)]
        with source.open('rb') as stream:
            index = start_id
            while True:
                data = stream.read(row_bytes * max(1, (8 * 1024 * 1024) // row_bytes))
                if not data:
                    break
                if len(data) % row_bytes:
                    raise ValueError('source changed during partitioning')
                input_hash.update(data)
                for offset in range(0, len(data), row_bytes):
                    shard = index % shard_count
                    row = memoryview(data)[offset:offset + row_bytes]
                    streams[shard].write(row)
                    hashes[shard].update(row)
                    counts[shard] += 1
                    index += 1
        if index - start_id != size // row_bytes:
            raise ValueError('source size changed during partitioning')
        for stream in streams:
            stream.flush()
            os.fsync(stream.fileno())
        result = {
            'source_id': source_id, 'source_sha256': input_hash.hexdigest(),
            'dimension': dimension, 'start_id': start_id, 'row_count': sum(counts),
            'id_stride': shard_count, 'elapsed_s': time.monotonic() - started,
            'shards': [dict(shard_id=str(i), vectors=str((output / f'shard-{i}.f32').resolve()),
                            row_count=counts[i], id_offset=i, id_stride=shard_count,
                            global_id_start=start_id + (i - start_id) % shard_count,
                            sha256=hashes[i].hexdigest()) for i in range(shard_count)]}
        # Only a completed preparation has a manifest; failed directories are not reusable.
        with (output / 'input.json').open('x') as stream:
            json.dump(result, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        return result
    finally:
        for stream in streams:
            stream.close()


def command(job):
    """Construct shell-safe invocation from a deliberately narrow job schema."""
    for field in ('binary', 'vectors', 'checkpoint_dir', 'shard_id', 'source_id'):
        if not isinstance(job.get(field), str) or not job[field]:
            raise ValueError('missing ' + field)
    for field in ('dimension', 'row_count', 'id_stride', 'generation'):
        if type(job.get(field)) is not int or job[field] <= 0:
            raise ValueError(field + ' must be a positive integer')
    if type(job.get('global_id_start')) is not int or job['global_id_start'] < 0:
        raise ValueError('global_id_start must be a nonnegative integer')
    if job.get('metric', 'l2') not in ('l2', 'cosine', 'ip'):
        raise ValueError('unsupported metric')
    args = [job['binary'], 'build']
    for field in ('vectors', 'checkpoint_dir', 'shard_id', 'source_id', 'dimension',
                  'row_count', 'global_id_start', 'id_stride', 'generation'):
        args.extend(['--' + field.replace('_', '-'), str(job[field])])
    args.extend(['--metric', job.get('metric', 'l2')])
    for field in ('threads', 'checkpoint_rows', 'branching_factor', 'split_threshold', 'merge_threshold'):
        if field in job:
            if type(job[field]) is not int or job[field] < (0 if field == 'merge_threshold' else 1):
                raise ValueError('invalid construction option: ' + field)
            args.extend(['--' + field.replace('_', '-'), str(job[field])])
    if job.get('resume'):
        args.append('--resume')
    host = job.get('host', 'local')
    if host == 'local':
        return args
    if not isinstance(host, str) or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.@-]*', host):
        raise ValueError('host must be a trusted SSH hostname or alias')
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', host, shlex.join(args)]


def run_jobs(jobs, log_dir, max_hosts=8, timeout=86400):
    """Run one build at a time per host, and different hosts concurrently.

    This does not publish a collection or retry a failed build. On an SSH timeout,
    the remote process may still run and must be inspected before retrying.
    """
    if max_hosts <= 0 or timeout <= 0 or not jobs:
        raise ValueError('nonempty jobs and positive limits are required')
    commands = [command(job) for job in jobs]
    identities = [(job.get('host', 'local'), str(Path(job['checkpoint_dir']).resolve())
                   if job.get('host', 'local') == 'local' else job['checkpoint_dir']) for job in jobs]
    if len(identities) != len(set(identities)) or len({j['shard_id'] for j in jobs}) != len(jobs):
        raise ValueError('one unique job per shard and checkpoint directory is required')
    if len({job['generation'] for job in jobs}) != 1:
        raise ValueError('all jobs must target the same generation')
    groups = {}
    for index, job in enumerate(jobs):
        groups.setdefault(job.get('host', 'local'), []).append(index)
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=False)
    (log_dir / 'jobs.json').write_text(json.dumps(jobs, indent=2) + '\n')
    started = time.monotonic()

    def host_jobs(indices):
        records = []
        remote_state_unknown = False
        for index in indices:
            begin = time.monotonic()
            row = {'shard_id': jobs[index]['shard_id'], 'host': jobs[index].get('host', 'local'),
                   'generation': jobs[index]['generation'], 'start_offset_s': begin - started}
            if remote_state_unknown:
                row.update(status='blocked', error='earlier SSH build may still run; reconcile the host before retrying', elapsed_s=0)
                records.append(row)
                continue
            with (log_dir / f'{index}.stdout').open('wb') as out, (log_dir / f'{index}.stderr').open('wb') as err:
                try:
                    result = subprocess.run(commands[index], stdout=out, stderr=err, timeout=timeout, check=False)
                    row.update(returncode=result.returncode, status='ok' if result.returncode == 0 else 'error')
                except subprocess.TimeoutExpired:
                    remote_state_unknown = jobs[index].get('host', 'local') != 'local'
                    row.update(status='timeout', remote_state_unknown=remote_state_unknown)
                except OSError as exc:
                    row.update(status='error', error=str(exc))
            row['elapsed_s'] = time.monotonic() - begin
            records.append(row)
        return records

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_hosts, len(groups))) as pool:
        records = [record for group in pool.map(host_jobs, groups.values()) for record in group]
    summary = dict(elapsed_s=time.monotonic() - started, jobs=records,
                   complete=all(row['status'] == 'ok' for row in records))
    (log_dir / 'build.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare = sub.add_parser('partition')
    for name in ('source', 'output', 'source-id'):
        prepare.add_argument('--' + name, required=True)
    for name in ('dimension', 'shard-count'):
        prepare.add_argument('--' + name, type=int, required=True)
    prepare.add_argument('--start-id', type=int, default=0)
    build = sub.add_parser('run')
    build.add_argument('--jobs', required=True, help='JSON array of structured build jobs')
    build.add_argument('--log-dir', required=True)
    build.add_argument('--max-hosts', type=int, default=8)
    build.add_argument('--timeout', type=float, default=86400)
    args = parser.parse_args()
    if args.command == 'partition':
        print(json.dumps(partition(args.source, args.output, args.dimension, args.shard_count, args.source_id, args.start_id)))
    else:
        result = run_jobs(json.loads(Path(args.jobs).read_text()), args.log_dir, args.max_hosts, args.timeout)
        print(json.dumps(result))
        raise SystemExit(0 if result['complete'] else 1)


if __name__ == '__main__':
    main()
