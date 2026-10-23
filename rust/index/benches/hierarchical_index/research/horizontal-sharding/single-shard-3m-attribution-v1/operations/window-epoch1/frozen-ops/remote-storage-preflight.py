#!/usr/bin/env python3
"""Expand only the verified approved EBS root filesystem; preserve every file."""
import argparse,json,pathlib,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--window-id',required=True);p.add_argument('--output',required=True,type=pathlib.Path);a=p.parse_args();assert a.window_id.replace('-','').isalnum()
def run(argv):return subprocess.check_output(argv,text=True).strip()
subprocess.run(['systemctl','is-active','--quiet',f'spann-single-shard-3m-{a.window_id}-stop.timer'],check=True)
source=run(['findmnt','-n','-o','SOURCE','/']);fs=run(['findmnt','-n','-o','FSTYPE','/']);part=pathlib.Path(source).resolve();diskname=run(['lsblk','-n','-o','PKNAME',str(part)]);disk=pathlib.Path('/dev')/diskname
assert diskname and '/' not in diskname and fs=='ext4'
serial=run(['lsblk','-d','-n','-o','SERIAL',str(disk)]);assert serial.replace('-','')=='vol08bda85902f848a75'
assert int(run(['lsblk','-d','-b','-n','-o','SIZE',str(disk)]))==256*1024**3
before=run(['df','-B1','/']);commands=[]
if int(run(['lsblk','-b','-n','-o','SIZE',str(part)]))<250*1024**3:
 number=run(['lsblk','-n','-o','PARTN',str(part)]);assert number=='1'
 cmd=['sudo','-n','growpart',str(disk),number];subprocess.run(cmd,check=True);commands.append(cmd)
cmd=['sudo','-n','resize2fs',str(part)];subprocess.run(cmd,check=True);commands.append(cmd)
assert int(run(['df','-B1','--output=size','/']).splitlines()[-1])>250*1024**3
report={'observed_unix':time.time(),'source':source,'resolved_partition':str(part),'disk':str(disk),'serial':serial,'filesystem':fs,'before':before,'after':run(['df','-B1','/']),'commands':commands,'retained_files_removed':False};a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
