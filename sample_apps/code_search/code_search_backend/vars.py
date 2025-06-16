import os
from dotenv import load_dotenv

load_dotenv()

REPO_NAME = "chroma-core/chroma"
COMMIT_HASH = "1ce0a4fd2eb8bb837a08f85566fd2ca9d2a9acfe"

# User-supplied variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
JINA_AI_API_KEY = os.getenv("JINA_AI_API_KEY")

# Other Parameters

MAX_CHROMA_BATCH_SIZE = 60
MAX_EMBEDDING_BATCH_SIZE = 10
