import ast,re
from pathlib import Path
root=Path(__file__).parent
outputs=[]
for variant in ['baseline','candidate']:
    results={}
    for line in (root/f'check-{variant}.stderr').read_text().splitlines():
        match=re.fullmatch(r'READ_RESULT phase=(cold|warm) query=(\d+) (.*)',line)
        if match:
            key=(match[1],int(match[2]))
            assert key not in results
            results[key]=ast.literal_eval(match[3])
    assert len(results)==200
    assert all(len(row)==100 for row in results.values())
    outputs.append(results)
assert outputs[0]==outputs[1]
print('PASS: 200 query/pass combinations; identical100 IDs, ordering, andfloat32score bits percombination.')
