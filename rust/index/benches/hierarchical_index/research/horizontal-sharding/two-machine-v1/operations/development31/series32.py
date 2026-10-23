exec(open(__file__.replace('series32.py','series31.py')).read().split('mode=sys.argv[1]')[0])
kind='epoch31v4'
run('v4-replay',mode='poll_1ms')
run('v4-control-detailed',mode='poll_1ms',flags=['detailed'])
for i in range(1,4):
 arms=[('workers64','64','0'),('refreshgate','32','1')]
 if i==2:arms.reverse()
 for name,workers,gate in arms:
  os.environ['SPANN_INSERTION_WORKERS']=workers
  os.environ['SPANN_SERIALIZE_BOUNDARY_REFRESH']=gate
  run(name+'-'+str(i),mode='poll_1ms')
os.environ['SPANN_INSERTION_WORKERS']='32'
os.environ['SPANN_SERIALIZE_BOUNDARY_REFRESH']='1'
run('refreshgate-detailed',mode='poll_1ms',flags=['detailed'])
os.environ['SPANN_SERIALIZE_BOUNDARY_REFRESH']='0'
run('save-serial',mode='poll_1ms',save_workers='1',flags=['save'])
run('save-parallel',mode='poll_1ms',save_workers='32',flags=['save'])
