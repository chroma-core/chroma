from pathlib import Path
import time,subprocess,sys
p=Path(__file__).resolve().parent;until=time.time()+2400
while not all((p/f'32-fixed-{arm}-profile'/'owner1-receipts.json').exists() for arm in ['off','on']):
 assert time.time()<until,'diagnostic pipeline incomplete'
 time.sleep(5)
subprocess.run([sys.executable,str(p/'collect-profiles.py')],check=True)
subprocess.run([sys.executable,str(p/'full-candidate-series.py')],check=True)
