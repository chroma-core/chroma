import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from .dynamic_archive import export_checkpoint,import_checkpoint,DEFAULT_LIMITS

class ArchiveTests(unittest.TestCase):
    def test_complete_regular_transfer_and_wire_digest(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'source';(source/'data').mkdir(parents=True);(source/'data/block').write_bytes(b'actual bytes');archive=root/'checkpoint.tar'
            receipt=export_checkpoint(source,archive);self.assertEqual(import_checkpoint(archive,root/'copy',receipt),receipt['files'])
            archive.write_bytes(archive.read_bytes()[:-1])
            with self.assertRaisesRegex(ValueError,'wire archive'):import_checkpoint(archive,root/'bad',receipt)
    def test_links_traversal_duplicates_and_expansion_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);payload=b'x';good=dict(bytes=1,sha256=hashlib.sha256(payload).hexdigest())
            for index,kind in enumerate(('traversal','symlink','duplicate','oversize')):
                archive=root/(kind+'.tar')
                with tarfile.open(archive,'w',format=tarfile.USTAR_FORMAT) as stream:
                    info=tarfile.TarInfo('../outside' if kind=='traversal' else 'block');info.size=1
                    if kind=='symlink':info.type=tarfile.SYMTYPE;info.linkname='../outside';info.size=0
                    stream.addfile(info,None if kind=='symlink' else io.BytesIO(payload))
                    if kind=='duplicate':stream.addfile(info,io.BytesIO(payload))
                receipt=dict(bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),files={'../outside' if kind=='traversal' else 'block':good})
                limits=dict(DEFAULT_LIMITS,file_bytes=0) if kind=='oversize' else DEFAULT_LIMITS
                with self.subTest(kind=kind),self.assertRaises(ValueError):import_checkpoint(archive,root/('target-'+kind),receipt,limits)
                self.assertFalse((root/'outside').exists())
    def test_expanded_and_wire_caps_apply_before_destination_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'source';source.mkdir();(source/'block').write_bytes(b'abcd')
            for key in ('wire_bytes','expanded_bytes','file_bytes','members'):
                limits=dict(DEFAULT_LIMITS);limits[key]=0
                with self.subTest(limit=key),self.assertRaises(ValueError):export_checkpoint(source,root/(key+'.tar'),limits)
