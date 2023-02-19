# Let's ask God some questions
from langchain.vectorstores import Chroma
from langchain.chains import VectorDBQAWithSourcesChain
from langchain.embeddings import OpenAIEmbeddings
from langchain import OpenAI

collection_name = 'the_bible'
persist_directory = 'chroma'

# Read in the oepnai api key from openai.key
openai_api_key = open('openai.key', 'r').read()

docsearch = Chroma(collection_name=collection_name, persist_directory=persist_directory, embedding_function=OpenAIEmbeddings(openai_api_key=openai_api_key))
chain = VectorDBQAWithSourcesChain.from_chain_type(OpenAI(temperature=0, openai_api_key=openai_api_key), chain_type="stuff", vectorstore=docsearch)

chain({"question": "What is the greatest good?"}, return_only_outputs=True)