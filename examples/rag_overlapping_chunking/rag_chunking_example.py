"""
RAG Pipeline with Overlapping Text Chunking using ChromaDB

A practical example showing how to ingest long documents, chunk them
with overlap to preserve context at boundaries, and retrieve relevant
sections for use as LLM context.
"""

import chromadb
from chromadb.utils import embedding_functions


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[dict]:
    """
    Split text into overlapping chunks.
    
    Overlap prevents losing context at chunk boundaries. For example,
    if a sentence spans two chunks, the overlap ensures the full
    sentence appears in at least one chunk.

    Args:
        text: The full document text to chunk.
        chunk_size: Number of characters per chunk.
        overlap: Number of characters to overlap between consecutive chunks.

    Returns:
        List of dicts with chunk text, start index, and end index.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append({
            "text": chunk,
            "start_index": start,
            "end_index": min(end, len(text)),
        })
        start += chunk_size - overlap
    return chunks


def build_collection(
    documents: list[dict],
    collection_name: str = "rag_documents",
) -> chromadb.Collection:
    """
    Chunk documents, embed them, and store in a ChromaDB collection.

    Each chunk is stored with metadata tracking its source document
    and position, which is useful for citation and deduplication.

    Args:
        documents: List of dicts with 'id', 'text', and optional 'metadata'.
        collection_name: Name of the ChromaDB collection.

    Returns:
        The populated ChromaDB collection.
    """
    client = chromadb.Client()

    collection = client.get_or_create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine"},
    )

    all_ids = []
    all_documents = []
    all_metadatas = []

    for doc in documents:
        chunks = chunk_text(doc["text"])
        for i, chunk in enumerate(chunks):
            chunk_id = f"{doc['id']}_chunk_{i}"
            all_ids.append(chunk_id)
            all_documents.append(chunk["text"])
            all_metadatas.append({
                "source_doc_id": doc["id"],
                "chunk_index": i,
                "start_char": chunk["start_index"],
                "end_char": chunk["end_index"],
                **(doc.get("metadata", {})),
            })

    collection.add(
        ids=all_ids,
        documents=all_documents,
        metadatas=all_metadatas,
    )

    return collection


def retrieve_context(
    collection: chromadb.Collection,
    query: str,
    n_results: int = 5,
) -> list[str]:
    """
    Query the collection and return the most relevant chunks.

    These chunks can be concatenated and passed as context to an LLM
    for summarization, question answering, or structured extraction.

    Args:
        collection: The ChromaDB collection to query.
        query: Natural language query string.
        n_results: Number of chunks to retrieve.

    Returns:
        List of relevant text chunks, ordered by similarity.
    """
    results = collection.query(
        query_texts=[query],
        n_results=n_results,
    )
    return results["documents"][0]


if __name__ == "__main__":
    documents = [
        {
            "id": "note_001",
            "text": (
                "Patient presents with persistent cough lasting three weeks. "
                "No fever or shortness of breath reported. Chest X-ray shows "
                "no acute findings. Assessment: likely post-viral cough. Plan: "
                "prescribed benzonatate 100mg TID, follow up in two weeks if "
                "symptoms persist. Patient advised to avoid irritants and stay "
                "hydrated. Will consider referral to pulmonology if no improvement."
            ),
            "metadata": {"type": "clinical_note", "department": "primary_care"},
        },
        {
            "id": "note_002",
            "text": (
                "Follow-up visit for type 2 diabetes management. HbA1c improved "
                "from 8.2 to 7.1 over the past three months. Current medications: "
                "metformin 1000mg BID, glipizide 5mg daily. Blood pressure 128/82. "
                "BMI 31.2, down from 32.5. Patient reports increased physical "
                "activity and dietary changes. Plan: continue current regimen, "
                "recheck HbA1c in three months, annual eye exam ordered."
            ),
            "metadata": {"type": "clinical_note", "department": "endocrinology"},
        },
    ]

    collection = build_collection(documents)

    query = "What medications is the diabetes patient taking?"
    context_chunks = retrieve_context(collection, query, n_results=3)

    print(f"Query: {query}")
    print(f"Retrieved {len(context_chunks)} relevant chunks:\n")
    for i, chunk in enumerate(context_chunks):
        print(f"  Chunk {i + 1}: {chunk[:120]}...")
