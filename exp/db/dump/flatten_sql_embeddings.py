#!/usr/bin/env python3

import json
import sys
from collections import defaultdict

# Key:Entry
buckets = defaultdict(list)

class Entry:
    def __init__(self, raw):
        embeddings = json.loads(raw['data'])
        raw['embeddings'] = embeddings['data']
        del raw['data']
        self.data = raw

    @property
    def bucket(self):
        return f"ps{self.data['projection_set_id']}_es{self.data['embedding_set_id']}_{self.data['project_name']}"

    @property
    def key(self):
        return self.data['embedding_id']

    @property
    def json(self):
        return json.dumps(self.data)

sql_output = json.loads(sys.stdin.read())
for row in sql_output:
    entry = Entry(row)
    buckets[entry.bucket].append(entry)

for bucket, entries in buckets.items():
    print(f"{bucket}, {len(entries)}")
    with open(f"{bucket}.jsonl", "w") as out:
        for entry in entries:
            out.write(entry.json)
