"""Validate the promising fixed-state candidate in full final-million construction."""
exec(open(__file__.replace('full-candidate-series.py','candidate-series.py')).read().split("run('32-instrumented-replay')")[0])
for i,arms in enumerate([('on','early'),('early','on'),('on','early')],1):
 for arm in arms:run(f'32-full-{arm}-{i}',arm)
