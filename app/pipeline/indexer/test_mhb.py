from .mhb import MHBIndexer

class TestDataset:
    count = 0
        
    def iterate(self):
        for i in range(10):
            yield i
            self.count += 1
            
class TestIndexWriter:
    write_calls = []
    
    def write(self, dataset, index):
        self.write_calls.append({dataset:dataset, index:index})

def test_mhb():
    indexer = MHBIndexer()
    dataset = TestDataset()
    writer = TestIndexWriter()
    indexer.index(dataset, writer)
    assert len(writer.write_calls) == 1
    assert dataset.count == 10