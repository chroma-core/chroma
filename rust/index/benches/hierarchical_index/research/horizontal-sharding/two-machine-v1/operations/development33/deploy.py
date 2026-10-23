from run import P,B,ssh,put,worker,peer
import json,shlex,time
h=json.loads((P/'epoch33v1-binary.json').read_text())['sha256']
for i in [worker,peer]:put(i,P/'binary-cache.py',B+'/binary-cache33.py')
for i,mode in [(worker,'upload'),(peer,'download')]:
 result=ssh(i,shlex.join(['python3',B+'/binary-cache33.py',mode,B+'/epoch33v1/two_machine_spann',h,'--deadline',str(int(time.time()+180))]),timeout=190)
 (P/(i+'-binary-transfer.json')).write_text(result)
print('DEPLOYED',h)
