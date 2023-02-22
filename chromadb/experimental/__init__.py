from typing import Optional
import chromadb
from chromadb.api import API
from chromadb.config import Settings
from chromadb.experimental.ExperimentalClient import ExperimentalClient as EClient

# A monkeypatch wrapper for the regular client. 
def ExperimentalClient(*args, **kwargs):
    
    # Output a pretty colorful message to the user
    experimental_message = f"""
     ###### \033[38;5;220m Welcome to the Chroma experimental client! \033[0m #####
     {chromadb._logo}

We'd love to know what you're experimenting with! Join the Discord:

               https://discord.gg/9WZAkTEEwC

    """ 
    print(experimental_message)

    return EClient(*args, **kwargs)