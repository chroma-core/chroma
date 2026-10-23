exec(open(__file__.replace('series33.py','series31.py')).read().split('mode=sys.argv[1]')[0])
kind='epoch31v4'
os.environ['SPANN_INSERTION_WORKERS']='64'
os.environ['SPANN_SERIALIZE_BOUNDARY_REFRESH']='1'
for i in range(1,4):run('combined-'+str(i),mode='poll_1ms')
run('combined-save',mode='poll_1ms',flags=['save'])
