"""Archive every declared preparation file and fully restore each pinned object."""
import concurrent.futures,hashlib,json,time
from pathlib import Path
from .tree_manifest import artifact,canonical_bytes,_atomic_write
from .artifact_store import file_checksums


def archive_and_restore(job,store,clock=time.time):
    output=Path(job['archive']['output']);output.mkdir(parents=True,exist_ok=False);restore=Path(job['archive']['restore']);restore.mkdir(parents=True,exist_ok=False)
    scopes=[Path(p) for p in job['archive']['scopes']];paths=[]
    for root in scopes:
        if not root.is_dir() or root.is_symlink():raise ValueError('archive scope must be a real directory')
        for path in sorted(root.rglob('*')):
            if path.is_symlink():raise ValueError('archive rejects symlinks')
            if path.is_file():paths.append((root,path))
    if len({str(p) for _,p in paths})!=len(paths):raise ValueError('archive scopes overlap')
    # Partition metadata and every failed attempt stay in the logical catalog.
    report=dict(status='running',files=[],started_epoch=clock(),scope='Full private file catalog including failed attempts. Every unique object receives complete restored SHA verification; retained restore output is owned.')
    _atomic_write(output/'report.json',canonical_bytes(report));catalog={};done={};original_checks={}
    try:
        for root,path in paths:
            checks=file_checksums(path)
            if (checks['bytes']+job['archive']['part_bytes']-1)//job['archive']['part_bytes']>job['archive']['maximum_part_count']:raise ValueError('artifact exceeds bounded multipart part count')
            catalog[(checks['bytes'],checks['sha256'])]=(path,checks);original_checks[str(path)]=checks
        def one(item):
            number,(path,checks)=item
            if clock()>=job['stop_epoch']-1800:raise TimeoutError('archive reached preservation reserve')
            descriptor=store.upload(path,part_bytes=job['archive']['part_bytes'])
            if descriptor['sha256']!=checks['sha256'] or descriptor['bytes']!=checks['bytes']:raise ValueError('source changes during archive')
            restored=store.restore(descriptor,restore/str(number))
            return (checks['bytes'],checks['sha256']),dict(artifact=descriptor,restore=restored)
        with concurrent.futures.ThreadPoolExecutor(max_workers=job['archive']['concurrency']) as pool:
            futures=[pool.submit(one,item) for item in enumerate(catalog.values())]
            for future in concurrent.futures.as_completed(futures):
                identity,row=future.result();done[identity]=row
                _atomic_write(output/'progress.json',canonical_bytes(dict(status='running',completed_objects=len(done),objects=len(catalog),completed=list(done.values()))))
        for root,path in paths:
            checks=file_checksums(path);identity=(checks['bytes'],checks['sha256'])
            if checks!=original_checks[str(path)]:raise ValueError('archive source changes after restore')
            if identity not in done:raise ValueError('archive source changes after restore')
            report['files'].append(dict(scope=str(root),relative_path=str(path.relative_to(root)),**done[identity]))
        # Re-enumeration detects omitted/new files; concurrent writers invalidate handoff.
        after={(str(root),str(p)) for root in scopes for p in root.rglob('*') if p.is_file()}
        if after!={(str(root),str(path)) for root,path in paths}:raise ValueError('archive source inventory changes')
        report.update(status='passed',finished_epoch=clock(),unique_objects=len(catalog),unique_bytes=sum(k[0] for k in catalog),logical_files=len(paths))
        _atomic_write(output/'catalog.json',canonical_bytes(report));desc=store.upload(output/'catalog.json',part_bytes=job['archive']['part_bytes']);store.restore(desc,restore/'catalog.json')
        _atomic_write(output/'catalog-object.json',canonical_bytes(desc));return report
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:_atomic_write(output/'report.json',canonical_bytes(report))
