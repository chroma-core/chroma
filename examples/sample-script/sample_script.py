from hashlib import new
import chroma_client

new_labels = chroma_client.fetch_new_labels()
print(new_labels)