from pathlib import Path
import time,subprocess,sys,json
p=Path(__file__).resolve().parent
until=time.time()+2400
while True:
 ready=all((p/f'32-gate-{arm}-{i}'/'owner1-receipts.json').exists() for i in range(1,7) for arm in ['off','on'])
 if ready:break
 assert time.time()<until,'baseline series incomplete; inspect retained logs'
 time.sleep(5)
# run.py collects only after both owned benchmark supervisors are stopped.
subprocess.run([sys.executable,str(p.parent/'development30/renew-lease.py')],check=True)
subprocess.run([sys.executable,str(p/'build.py')],check=True)
subprocess.run([sys.executable,str(p/'candidate-series.py')],check=True)
