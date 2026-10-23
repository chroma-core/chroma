"""No silent policy fallback and no unsupported scoring-policy mixture."""
import unittest
from unittest.mock import Mock
from .subtree_transport import PinnedReader,TreeError

class PostingSelectionContract(unittest.TestCase):
    def test_invalid_policy_and_streaming_mix_reject_before_spawn(self):
        for flags in (dict(posting_selection_policy=True),dict(posting_selection_policy='unknown'),dict(posting_selection_policy='partition-inclusive',streaming_limits={})):
            with self.assertRaises(ValueError):PinnedReader(dict(snapshot_sha256='x',**flags))
    def test_old_worker_missing_echo_rejects(self):
        pinned=PinnedReader.__new__(PinnedReader);pinned.descriptor={'snapshot_sha256':'hash'};pinned.snapshot={'generation':1,'shard_id':'a'};pinned.provider_cache_policy=None;pinned.streaming_limits=None;pinned.posting_selection_policy='partition-inclusive';pinned.reader=Mock();pinned.reader.query.return_value={}
        with self.assertRaisesRegex(TreeError,'posting selection policy'):pinned.call(dict(snapshot_sha256='hash',subtree_id='a',payload=dict(op='expand')))
