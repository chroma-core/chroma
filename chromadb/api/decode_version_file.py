from chromadb.proto.coordinator_pb2 import CollectionVersionFile

msg = CollectionVersionFile()

with open('/Users/sanketkedia/Downloads/000394_37c7a88a-bec1-440d-a794-2745e2e62975_flush', 'rb') as f:
    msg.ParseFromString(f.read())
    print(msg)