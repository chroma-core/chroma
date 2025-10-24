#!/usr/bin/env python3
"""
Example: Using Chroma's Attached Functions API to process collections automatically

This demonstrates how to attach functions that automatically process
collections as new records are added.
"""

import chromadb
import time

# Connect to Chroma server
client = chromadb.HttpClient(host="localhost", port=8000)
# ignore error if collection does not exist
try:
    client.delete_collection("my_documents_counts")
except Exception:
    pass
# Create or get a collection
collection = client.get_or_create_collection(
    name="my_document", metadata={"description": "Sample documents for task processing"}
)

# Add some sample documents
collection.add(
    ids=["doc1", "doc2", "doc3"],
    documents=[
        "The quick brown fox jumps over the lazy dog",
        "Machine learning is a subset of artificial intelligence",
        "Python is a popular programming language",
    ],
    metadatas=[{"source": "proverb"}, {"source": "tech"}, {"source": "tech"}],
)

print(f"✅ Created collection '{collection.name}' with {collection.count()} documents")

# Attach a function that counts records in the collection
# The 'record_counter' function processes each record and outputs {"count": N}
attached_fn = collection.attach_function(
    function_id="record_counter",  # Built-in function that counts records
    name="count_my_docs",
    output_collection="my_documents_counts",  # Auto-created
    params=None,  # No additional parameters needed
)

print("✅ Function attached successfully!")
print(f"   Attached Function ID: {attached_fn.id}")
print(f"   Name: {attached_fn.name}")
print(f"   Function: {attached_fn.function_id}")
print(f"   Input collection: {collection.name}")
print(f"   Output collection: {attached_fn.output_collection}")

# The function will now run automatically when:
# 1. New documents are added to 'my_documents'
# 2. The number of new records >= min_records_for_invocation (default: 100)

print("\n" + "=" * 60)
print("Function is now attached and will run on new data!")
print("=" * 60)

time.sleep(10)

# Add more documents to trigger function execution
print("\nAdding more documents...")
collection.add(
    ids=["doc4", "doc5"],
    documents=["Chroma is a vector database", "Functions automate data processing"],
)

print(f"Collection now has {collection.count()} documents")

# Later, you can detach the function
print("\n" + "=" * 60)
input("Press Enter to detach the function...")

success = attached_fn.detach(
    delete_output_collection=True  # Also delete the output collection
)

if success:
    print("✅ Function detached successfully!")
else:
    print("❌ Failed to detach function")
