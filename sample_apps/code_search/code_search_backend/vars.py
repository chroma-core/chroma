import os
from dotenv import load_dotenv

load_dotenv()

# User-supplied variables
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
JINA_AI_API_KEY = os.getenv('JINA_AI_API_KEY')

CHROMA_COLLECTION_NAME = "code-search-collection"

# Other Parameters

MAX_CHROMA_BATCH_SIZE = 5461
MAX_EMBEDDING_BATCH_SIZE = 10
