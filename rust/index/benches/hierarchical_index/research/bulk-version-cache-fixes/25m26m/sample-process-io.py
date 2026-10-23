import json,pathlib,subprocess,sys,time
out=pathlib.Path(sys.argv[1]);start=time.monotonic();p=subprocess.Popen(sys.argv[2:]);maximum={};samples=0
while True:
 try:
  vals={k:int(v) for k,v in (line.split(':') for line in pathlib.Path(f'/proc/{p.pid}/io').read_text().splitlines())}
  for k,v in vals.items():maximum[k]=max(v,maximum.get(k,0))
  samples+=1
 except OSError:pass
 code=p.poll()
 if code is not None:break
 time.sleep(.05)
out.write_text(json.dumps({'exit_code':code,'elapsed_seconds':time.monotonic()-start,'io_counter_maxima':maximum,'samples':samples,'note':'Counters are sampled from the benchmark process. rchar includes reads served by the OS file cache; read_bytes measures filesystem I/O.'},indent=2));sys.exit(code)
