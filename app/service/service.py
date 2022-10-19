from functools import lru_cache

class Services:
    def __init__(self, dataset, mhb_indexer):
        self.dataset = dataset
        self.mhb_indexer = mhb_indexer

    def Self(self):
        '''Return self for Pydantic'''
        return self

