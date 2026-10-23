# Sanity check script to ensure that the Chroma client can connect
# and is capable of recieving data.
import chromadb

# run in in-memory mode
chroma_api = chromadb.Client()
print(chroma_api.heartbeat())
