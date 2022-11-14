import json
from chroma_client import Chroma

chroma = Chroma()
chroma.reset()

# add
for i in range(10):
    chroma.add(
        embedding_data=[1,2,3,4,5,6,7,8,9,10],
        input_uri="https://www.google.com",
        dataset=None
    )

# fetch all
allres = chroma.get_all()
print(allres)

# count
print("count is", chroma.count()['count'])

# persist
chroma.persist()

# heartbeat
print(chroma.heartbeat())

# rand
print(chroma.rand())

# process
chroma.process()

# reset
chroma.reset()
print("count is", chroma.count()['count'])