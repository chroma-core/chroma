#!/usr/bin/env python3
"""
Example: Using Chroma's Task API to process collections automatically

This demonstrates how to register tasks that automatically process
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

# Create a task that counts records in the collection
# The 'record_counter' operator processes each record and outputs {"count": N}
success, task_id = collection.create_task(
    task_name="count_my_docs",
    operator_name="record_counter",  # Built-in operator that counts records
    output_collection_name="my_documents_counts",  # Auto-created
    params=None,  # No additional parameters needed
)
assert success
if success:
    print("✅ Task created successfully!")
    print(f"   Task ID: {task_id}")
    print("   Task name: count_my_docs")
    print(f"   Input collection: {collection.name}")
    print("   Output collection: my_documents_counts")
    print("   Operator: record_counter")
else:
    print("❌ Failed to create task")

# The task will now run automatically when:
# 1. New documents are added to 'my_documents'
# 2. The number of new records >= min_records_for_task (default: 100)

print("\n" + "=" * 60)
print("Task is now registered and will run on new data!")
print("=" * 60)

time.sleep(10)

# Add more documents to trigger task execution
print("\nAdding more documents...")
collection.add(
    ids=["doc4", "doc5"],
    documents=["Chroma is a vector database", "Tasks automate data processing"],
)

print(f"Collection now has {collection.count()} documents")

# Later, you can remove the task
print("\n" + "=" * 60)
input("Press Enter to remove the task...")

success = collection.remove_task(
    task_name="count_my_docs", delete_output=True  # Also delete the output collection
)

if success:
    print("✅ Task removed successfully!")
else:
    print("❌ Failed to remove task")
