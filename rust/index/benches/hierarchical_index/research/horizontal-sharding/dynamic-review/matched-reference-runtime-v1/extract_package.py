"""Extract a fresh reviewed package and validate its complete source inventory."""
import argparse,hashlib,json,os,tarfile
from pathlib import Path

def extract(archive,destination):
 destination=Path(destination)
 if destination.exists():raise ValueError('Fresh extraction destination required')
 destination.mkdir(parents=False)
 with tarfile.open(archive)as t:
  members=t.getmembers();names=set()
  if len(members)>6000 or sum(m.size for m in members)>600000000:raise ValueError('Archive bound exceeded')
  for m in members:
   p=Path(m.name)
   if p.is_absolute()or'..'in p.parts or m.name in names or not(m.isfile()or m.isdir()or m.issym()):raise ValueError('Unsafe archive member')
   names.add(m.name)
   if m.issym()and(m.name!='source/rust/types/idl'or m.linkname!='../../idl'):raise ValueError('Unexpected symlink')
  # Links are created last so no regular file can traverse one during extraction.
  for m in members:
   if not m.issym():t.extract(m,destination)
  for m in members:
   if m.issym():os.symlink(m.linkname,destination/m.name)
 from run_reference import verify_source
 import time
 verify_source(destination,time.time()+3600)
 return destination
if __name__=='__main__':
 a=argparse.ArgumentParser();a.add_argument('archive');a.add_argument('destination');v=a.parse_args();extract(v.archive,v.destination)
