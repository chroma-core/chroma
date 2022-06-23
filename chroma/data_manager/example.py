import numpy as np
from data_manager import ChromaDataManager

manager = ChromaDataManager()

# Set some attributes
manager.set_metadata(
    input_identifiers="input_id", inference_identifiers="inference_id", labels="label"
)

# Generate a random 1000 element float vector
embedding = np.random.rand(
    1000,
)
embedding = embedding.tolist()

# Try to save it
create_result = manager.store_embedding(data=embedding)
print(str(create_result))

# See if it's there
get_result = manager.get_embeddings()
print(str(get_result))
