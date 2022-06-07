from data_manager import ChromaDataManager
import numpy as np

manager = ChromaDataManager()

# Generate a random 1000 element float vector
embedding = np.random.rand(1000,)
embedding = embedding.tolist()

# Try to save it 
create_result = manager.store_embedding(data=embedding)
print(str(create_result))

# See if it's there
get_result = manager.get_embeddings()
print(str(get_result))