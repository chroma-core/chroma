# import pandas as pd
# import chromadb.proto.chroma_pb2 as chroma_pb2

# path = '/Users/sanketkedia/Downloads/FragmentSeqNo=0000000000000001.parquet'
# df = pd.read_parquet(path)
# body = df['body'].iloc[0]
# log_record = chroma_pb2.OperationRecord()
# err = log_record.ParseFromString(body)
# print(log_record)



import sys
from chromadb.proto.chroma_pb2 import Operation, OperationRecord

hex_line = "0a046665667712001a1b0a190a0f6368726f6d613a646f63756d656e7412060a0465667765"

def decode_file():
    data = bytes.fromhex(hex_line)
    # Decode data to OperationRecord and print
    record = OperationRecord.FromString(data)
    print(record)
    print(record.operation)

if __name__ == "__main__":
    decode_file()