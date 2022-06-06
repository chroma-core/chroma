from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

import gorilla
import pprint

def patched_pprint(self): 
    print("hello monkeys!")
    pass

class Chroma:

    def __init__(self):
        print("Chroma inititated, FYI: monkey patching pprint")
        settings = gorilla.Settings(allow_hit=True, store_hit=True)
        patch_pprint = gorilla.Patch(pprint, "pprint", patched_pprint, settings)
        gorilla.apply(patch_pprint)
