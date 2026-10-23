import pathlib,json,hashlib,tarfile,subprocess
root=pathlib.Path('/home/ubuntu/spann-single-shard-3m-attribution-v1/epoch1')
subprocess.run(['systemctl','is-active','--quiet','spann-single-shard-3m-epoch1-stop.timer'],check=True)
expected={'candidate.tar.gz': '47c3c01cb8c295beb6563956ffac1e224960bd0cd22d5f4bf0dffda02196ec08', 'reference.tar.gz': '6814ae6f0bf7259641a7884c10f02bbccd91e5c33dadb99132ded627f68bc137', 'remote-manifest.json': 'b24fa5776e0e3b3ca1ca2a2fcd81e585f7cb936439a9cce37d30a0585ec6370e', 'input-plan.json': '401542791f43e5bb80fd4aa4e94fef7be727db09408fac35bfde5ce78ea7756c', 'remote-storage-preflight.py': '06b4d59f2aa0356fab14eba45c4f45226fc29d34fa2bd37ab5d8aaa6b1a0ab31', 'materialize-input.py': '7948a9ef6acad39c1066f2be04d41500299c8443f22af9431dbcdf55b360fdd1', 'remote-perf-probe.py': '1cc806117242790758bdc1b329ffe9122708d3a07be01a16cbda1ff766df3a77', 'remote-run.py': '7ea8d20090cbe13a74bdce72f2aa83b6330b8555eb316df2548f83056fc32d77', 'remote-run.sh': '92412789d829627d0ad96048efad86d226b5636c6a69b40fde5a8be6a7e1b80f', 'perf-phase-summary.py': '31d6b11cf7da1196ad24fcf98aa613b0840cf6352f12ba908d1e239a647f299d'}
for name,digest in expected.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
for arm in ['reference','candidate']:
 dest=root/arm;dest.mkdir(exist_ok=False)
 with tarfile.open(root/(arm+'.tar.gz')) as archive:
  names=set()
  for member in archive.getmembers():
   name=pathlib.PurePosixPath(member.name);assert member.isfile() and not name.is_absolute() and '..' not in name.parts and member.name not in names;names.add(member.name)
  archive.extractall(dest,filter='data')
print(json.dumps({'all_uploaded_hashes_verified':expected,'safe_extract_complete':True}))
