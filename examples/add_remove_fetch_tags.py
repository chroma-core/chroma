import chroma
from chroma.sdk import chroma_manager

import json
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import TerminalFormatter

def _print(json_results):
    json_object = json.loads('{"foo":"bar"}')
    json_str = json.dumps(json_results, indent=4, sort_keys=True)
    print(highlight(json_str, JsonLexer(), TerminalFormatter()))

chroma_sdk = chroma_manager.ChromaSDK()

# 6695 is the tippytop of "Taiwan" / the 1s in MNIST
chroma_sdk.append_tag_by_name_to_datapoints_mutation("fromsdk", [2])
chroma_sdk.append_tag_by_name_to_datapoints_mutation("fromsdk2", [2])
chroma_sdk.remove_tag_by_name_from_datapoints_mutation("fromsdk2", [2])

datapoints_with_tag_fromsdk = chroma_sdk.get_datapoints(tagName="fromsdk", datasetId=1)
_print((datapoints_with_tag_fromsdk))
