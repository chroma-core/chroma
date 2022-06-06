import pprint
from chroma import chroma

chroma = chroma.chroma()
chroma.get_embeddings()
chroma.create_embeddings("this is from pip!")
pprint.pprint('this should trigger an additional print')