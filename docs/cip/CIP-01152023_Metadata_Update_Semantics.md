# The issue

```python
import chromadb

client = chromadb.PersistentClient()  # this is in-memory client, adjust as per your needs
collection = client.get_or_create_collection("mytest")
collection.add(ids=["id1"], documents=["document 1"], metadatas=[{"key_to_keep": 1, "key_to_remove": 2}])
records = collection.get(ids=["id1"])
print(records["metadatas"][0])
# {'key_to_keep': 1, 'key_to_remove': 2}
del records["metadatas"][0]["key_to_remove"]  # remove the unnecessary key
print(records)
# {'ids': ['id1'], 'embeddings': None, 'metadatas': [{'key_to_keep': 1}], 'documents': ['document 1'], 'uris': None, 'data': None}
collection.update(ids=records["ids"], documents=records["documents"], embeddings=records["embeddings"],
                  metadatas=records["metadatas"])
# verify
records1 = collection.get(ids=["id1"])
print(records1["metadatas"][0])
# {'key_to_keep': 1, 'key_to_remove': 2}
```

## The fix

We want to support three scenarios:

- Metadata for the item is None - the metadata for that item should be deleted from `embedding_metadata`
- Metadata is not provided with the update/upsert - No changes to the metadata on any of the items being update/upserted
- Metadata key is set to None - only the key should be deleted from the metadata for that item, rest of the keys should
  be preserved

Suggested approach involves supporting `NoneType` as metadata key value and the support of special metadata value that
is inserted at segment level to indicate that the metadata key should be deleted. We call this special
value `___METADATA_TOMBSTONE___` to appropriately reflect its intent. Our suggestion is for this special value to be
also documented in the API docs. The reason for documenting it is to make users aware of it and that it can be used as a
substitute for `NoneType` in metadata. The use of the tombstone value is inspired by XML Schema implementation
of [explicit nulls](https://www.w3.org/TR/xmlschema-1/#:~:text=2.6.&text=XML%20Schema%3A%20Structures%20introduces%20a,by%20the%20corresponding%20complex%20type.),
where a specific value is sent over the wire to indicate that the value should be deleted (`xsi:nil="true"`).

Here are examples of the three scenarios:

- Metadata for the record is None - the metadata for that item should be deleted from `embedding_metadata`

```python
import chromadb

client = chromadb.Client()

col = client.get_or_create_collection("test", metadata={"test": True})
col.add(ids=["1"], documents=["test-meta-none"], metadatas=[{"test": True}])
col.update(ids=["1"], documents=["test"], metadatas=[None])
res = col.get(ids=["1"])
print(res)
assert res["metadatas"][0] is None
```

- Metadata is not provided with the update/upsert - No changes to the metadata on any of the records being
  updated/upserted

> Note: The reason we want to support this is to preserve existing behavior and operations of user workflows.

```python
import chromadb

client = chromadb.Client()

col = client.get_or_create_collection("test-no-meta", metadata={"test": True})
col.add(ids=["1"], documents=["test-no-meta"], metadatas=[{"test": True, "test1": False}])
print(col.get(ids=["1"]))
col.update(ids=["1"], documents=["test1"])  # this is a bug that removes all the metadata
res = col.get(ids=["1"])
print(res)
assert res["metadatas"][0] == {"test": True, "test1": False}
```

- Metadata key is set to None - only the key should be deleted from the metadata for that record, rest of the keys
  should be preserved

> Note: Given the existing codebase we assume that this is the actual intended behavior which is only prevented by the
> lack of support for `NoneType` as metadata key value validation.

```python
import chromadb

client = chromadb.Client()

col = client.get_or_create_collection("test-partial", metadata={"test": True})
col.add(ids=["1"], documents=["test"], metadatas=[{"test": True, "test1": False}])
print(col.get(ids=["1"]))
col.update(ids=["1"], documents=["test"], metadatas=[{"test1": None}])
res = col.get(ids=["1"])
assert res["metadatas"][0] == {"test": True}
```