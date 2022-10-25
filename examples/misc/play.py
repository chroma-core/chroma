import json
from chroma_client import Chroma

chroma = Chroma(app="yolov3", model_version="1.0.0", layer="pool5")
chroma.reset()

# log
for i in range(10):
    chroma.log(
        embedding_data=[1,2,3,4,5,6,7,8,9,10],
        metadata={"test": "test"},
        input_uri="https://www.google.com",
        inference_data={"test": "test"},
        app="yolov3",
        model_version="1.0.0",
        layer="pool5",
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