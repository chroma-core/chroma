from pathlib import Path
import subprocess,sys,json,time
p=Path(__file__).resolve().parent
until=time.time()+1200
while not all((p/('epoch33v3-'+i+'-binary-transfer.json')).exists() for i in ['i-011ff180f03584f40','i-00268d1e1dfea0357']):
 assert time.time()<until,'build or transfer not complete; inspect logs'
 time.sleep(3)
subprocess.run([sys.executable,str(p/'series-v3.py')],check=True)
subprocess.run([sys.executable,str(p/'summarize-v3.py')],check=True)
subprocess.run([sys.executable,str(p/'diagnostics-v3.py')],check=True)
