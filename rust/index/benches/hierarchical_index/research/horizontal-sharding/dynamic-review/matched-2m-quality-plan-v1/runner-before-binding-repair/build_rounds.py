"""Run one projected treatment sequentially under external machine guards.

No cloud provisioning. An approved Linux binary and prepared projection are
required. Failed attempts remain in their exclusive output directory.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
from project_rounds import digest


def command(binary, job, checkpoint, dimension, threads, source_id, boundary_hash):
    args = [str(binary), 'build', '--vectors', job['vectors'], '--generation', str(job['generation']),
            '--source-id', source_id, '--dimension', str(dimension), '--row-count', str(job['batch_rows']),
            '--id-map', job['id_map'], '--checkpoint-dir', str(checkpoint), '--shard-id', str(job['owner']),
            '--metric', 'ip', '--threads', str(threads), '--branching-factor', '100',
            '--split-threshold', '2048', '--merge-threshold', '512',
            '--checkpoint-rows', str(job['batch_rows']), '--boundary-hash', boundary_hash]
    if job['resume']:
        args.append('--resume')
    return args


def build(projection_path, binary, binary_sha256, output, source_id, boundary_hash,
          threads, deadline_unix, min_free_bytes, max_process_rss_bytes, guard_timer):
    projection_path, binary, output = map(Path, (projection_path, binary, output))
    if threads < 1 or min_free_bytes < 1 or max_process_rss_bytes < 1:
        raise ValueError('positive resource limits required')
    if not source_id or not boundary_hash or not guard_timer:
        raise ValueError('source, boundary and external guard identity required')
    if digest(binary) != binary_sha256:
        raise ValueError('binary hash mismatch')
    projection_hash = digest(projection_path)
    projection = json.loads(projection_path.read_text())
    output.mkdir(parents=True, exist_ok=False)
    results = []
    for index, job in enumerate(projection['jobs']):
        if time.time() + 60 >= deadline_unix:
            raise RuntimeError('insufficient remaining fixed window')
        subprocess.run(['systemctl', 'is-active', '--quiet', guard_timer], check=True)
        if digest(projection_path) != projection_hash:
            raise ValueError('projection changed')
        for path, expected in [(binary, binary_sha256), (Path(job['vectors']), job['vectors_sha256']),
                               (Path(job['id_map']), job['id_map_sha256'])]:
            if digest(path) != expected:
                raise ValueError('immutable input changed')
        if shutil.disk_usage(output).free < min_free_bytes:
            raise RuntimeError('disk floor before build')
        args = command(binary, job, output / 'indexes' / str(job['owner']),
                       projection['dimension'], threads, source_id, boundary_hash)
        started = time.time(); peak = 0; reason = None
        env = dict(os.environ, RAYON_NUM_THREADS=str(threads), TOKIO_WORKER_THREADS=str(threads))
        with (output/f'{index:04}.stdout').open('xb') as out, (output/f'{index:04}.stderr').open('xb') as err:
            proc = subprocess.Popen(args, stdout=out, stderr=err, env=env, start_new_session=True)
            try:
                while proc.poll() is None:
                    status = Path(f'/proc/{proc.pid}/status')
                    try:
                        rss = next((int(line.split()[1])*1024 for line in status.read_text().splitlines()
                                    if line.startswith('VmRSS:')), 0)
                    except FileNotFoundError:
                        rss = 0
                    peak = max(peak, rss)
                    if time.time() >= deadline_unix - 30: reason = 'absolute deadline'
                    if shutil.disk_usage(output).free < min_free_bytes: reason = 'disk floor'
                    if rss > max_process_rss_bytes: reason = 'sampled process RSS limit'
                    if reason: break
                    time.sleep(.25)
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    try: proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL); proc.wait()
        results.append(dict(job=index, command=args, started_unix=started, elapsed_s=time.time()-started,
                            returncode=proc.returncode, stop_reason=reason, sampled_peak_rss_bytes=peak))
        (output/'build-results.json').write_text(json.dumps(results, indent=2)+'\n')
        if reason or proc.returncode != 0:
            raise RuntimeError('build stopped; retained partial evidence')
        manifest = json.loads((output/'indexes'/str(job['owner'])/'manifest.json').read_text())
        if manifest['vector_count'] != job['prior_rows'] + job['batch_rows'] or manifest['generation'] != job['generation']:
            raise ValueError('saved count/generation mismatch')
    return results
