import chroma
from chroma.sdk import chroma_manager

chroma_sdk = chroma_manager.ChromaSDK()

# 6695 is the tippytop of "Taiwan" / the 1s in MNIST
chroma_sdk.append_tag_by_name_to_datapoints_mutation("fromsdk", [6695])
chroma_sdk.append_tag_by_name_to_datapoints_mutation("fromsdk2", [6695])
chroma_sdk.remove_tag_by_name_from_datapoints_mutation("fromsdk2", [6695])

datapoints_with_tag_fromsdk = chroma_sdk.get_datapoints(tagName="fromsdk", datasetId=1)
print(str(datapoints_with_tag_fromsdk))
