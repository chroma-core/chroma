"""Targeted concurrency experiment; launch only after reviewing phase evidence."""
exec(open(__file__.replace('concurrency-series.py','candidate-series.py')).read().split("run('32-instrumented-replay')")[0])
for i,order in enumerate([(32,24),(24,32),(32,24)],1):
 for workers in order:
  os.environ['SPANN_BALANCE_WORKERS']=str(workers)
  run(f'32-concurrency-{workers}-{i}','on',['balance-only'])
