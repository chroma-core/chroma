from chromadb.proto.coordinator_pb2 import CollectionVersionFile


# Read the serialized data
with open('/Users/sanketkedia/Downloads/000006_2d6e15b6-0288-487e-b90d-943cd49b8405_flush', 'rb') as f:
    serialized_data = f.read()

# Create an instance and parse
history = CollectionVersionFile()
history.ParseFromString(serialized_data)

print(history)
# # Access the data
# for version in history.versions:
#     print(version)  # This will print each CollectionVersionInfo