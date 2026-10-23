"""Recover only a missing perf data.size after verified complete event records. Preserve original."""
import sys,struct,json,shutil,collections
from pathlib import Path
src,dst=map(Path,sys.argv[1:3]); data=src.read_bytes();header=struct.unpack_from('<13Q',data);offset=header[5]
assert header[6]==0 and header[0]==3622385352885552464
pos=offset;counts=collections.Counter()
while pos<len(data):
 kind,misc,size=struct.unpack_from('<IHH',data,pos)
 assert size>=8 and pos+size<=len(data)
 counts[kind]+=1;pos+=size
assert pos==len(data) and counts[9]>1000
assert not dst.exists()
shutil.copyfile(src,dst)
with dst.open('r+b') as f:f.seek(48);f.write(struct.pack('<Q',len(data)-offset))
print(json.dumps({'original_data_size':0,'data_offset':offset,'file_size':len(data),'valid_record_end':pos,'record_counts':dict(counts),'repair':'Copy original; set previously unwritten data.size header to verified complete event region length; no event records modified'},indent=2))
