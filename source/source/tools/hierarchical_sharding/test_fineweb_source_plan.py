import unittest
from .fineweb_source_plan import plan_prefix
from .test_fineweb_stream import fixture


class SourcePlanTests(unittest.TestCase):
    def test_actual_metadata_offsets_and_partial_final_groups(self):
        _,manifest,opener=fixture();result=plan_prefix(manifest,21,open_source=opener)
        self.assertEqual([(f['parquet_rows'],f['start_id'],f['row_count']) for f in result['files']],[(11,0,11),(12,11,10)])
        self.assertGreater(result['projected_compressed_column_bytes'],0);self.assertEqual(result['count'],21)

    def test_file_and_column_caps_fail_closed(self):
        _,manifest,opener=fixture()
        with self.assertRaisesRegex(ValueError,'file-count'):plan_prefix(manifest,21,max_files=1,open_source=opener)
        with self.assertRaisesRegex(ValueError,'read-memory'):plan_prefix(manifest,21,max_column_read_bytes=1,open_source=opener)
        with self.assertRaises(ValueError):plan_prefix(manifest,24,open_source=opener)

if __name__=='__main__':unittest.main()
