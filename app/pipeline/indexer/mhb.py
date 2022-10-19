import logging

from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class IndexWriter:
    def write(self, dataset, index):
        with open(self.index_path(dataset), 'w') as file:
            file.write(index)
    
    def index_path(self, dataset):
        return Path(dataset.fullpath).with_suffix(".mhb")

class MHBIndexer:
    def index(self, dataset, writer=IndexWriter()):
        logger.warning("Indexing dataset %s", dataset)
        count = 0
        for chunk in dataset.iterate():
            count += 1
        index_output = f"pretended to index {count} rows\n"
        writer.write(dataset, index_output)
        # with open(self.index_path(dataset), 'w') as file:
        #     file.write(f"pretended to index {count} rows\n")
