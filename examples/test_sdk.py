import chroma
from chroma.sdk import chroma_manager

chroma_sdk = chroma_manager.ChromaSDK()
print("chroma_sdk.get_projects()" + str(chroma_sdk.get_projects()))
