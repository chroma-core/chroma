from chromadb.proto.coordinator_pb2 import CollectionVersionFile

msg = CollectionVersionFile()

with open('/Users/sanketkedia/Downloads/000444_c5563dce-4708-46d2-8856-9641dfc0d017_flush', 'rb') as f:
    msg.ParseFromString(f.read())
    print(msg)