from pathlib import Path
r=Path('/mnt/msmarco/version-free-writer-7927-9b9c7eb')
exec((r/'run.py').read_text().split('\nfor dataset in ')[0])
subprocess.run(['bash',str(r/'audit.sh')],check=True)
(a/'audits-complete').write_text(str(time.time()))
while not (r/'scale-ready').exists():time.sleep(10)
exec((r/'scale.py').read_text())
