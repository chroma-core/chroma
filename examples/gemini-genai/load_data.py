import os
import argparse

from tqdm import tqdm

import chromadb
from chromadb.utils import embedding_functions
from google import genai

GENAI_EMBED_MODEL = 'gemini-embedding-001'

def main(
    documents_directory: str = "documents",
    collection_name: str = "documents_collection",
    persist_directory: str = ".",
) -> None:
    # Read all files in the data directory
    documents = []
    metadatas = []
    files = os.listdir(documents_directory)
    for filename in files:
        with open(f"{documents_directory}/{filename}", "r") as file:
            for line_number, line in enumerate(
                tqdm((file.readlines()), desc=f"Reading {filename}"), 1
            ):
                # Strip whitespace and append the line to the documents list
                line = line.strip()
                # Skip empty lines
                if len(line) == 0:
                    continue
                documents.append(line)
                metadatas.append({"filename": filename, "line_number": line_number})
    print(f'Read {len(documents)} documents (document lines).' )
    
    # Instantiate a persistent chroma client in the persist_directory.
    # Learn more at docs.trychroma.com
    client = chromadb.PersistentClient(path=persist_directory)

    if "GOOGLE_API_KEY" not in os.environ:
        gapikey = input("Please enter your Google API Key: ")
        #genai.configure(api_key=gapikey)
        #google_api_key = gapikey
        os.environ['GOOGLE_API_KEY'] = gapikey

    # create embedding function
    embedding_function = embedding_functions.GoogleGenaiEmbeddingFunction(model_name=GENAI_EMBED_MODEL)

    # If the collection already exists, we just return it. This allows us to add more
    # data to an existing collection.
    collection = client.get_or_create_collection(
        name=collection_name, embedding_function=embedding_function
    )

    # Create ids from the current count
    orig_collection_document_count = collection.count()
    print(f"Collection already contains {orig_collection_document_count} documents")
    if orig_collection_document_count < len(documents):
        print('Adding remaining documents to collection')
        ids = [str(i) for i in range(orig_collection_document_count, len(documents))]

        batch_size = 5  # Using small batch size to work better with Free Tier
        if batch_size > 1:
            # Load the documents in batches
            for i in tqdm(
                range(orig_collection_document_count, len(documents), batch_size), desc="Adding documents", unit_scale=batch_size):
                collection.add(
                    ids=ids[i : i + batch_size],
                    documents=documents[orig_collection_document_count : orig_collection_document_count + batch_size],
                    metadatas=metadatas[orig_collection_document_count : orig_collection_document_count + batch_size],
                )
        else:
            # Load the documents individually
            indicator_size = 10
            for i in range(orig_collection_document_count, len(documents)):
                collection.add(
                    ids=ids[i],
                    documents=documents[orig_collection_document_count+i],
                    metadatas=metadatas[orig_collection_document_count+i],
                )
                if (i%indicator_size) == 0:
                    print('.', end='')
            print()

    new_count = collection.count()
    print(f"Added {new_count - orig_collection_document_count} documents")


if __name__ == "__main__":
    # Read the data directory, collection name, and persist directory
    parser = argparse.ArgumentParser(
        description="Load documents from a directory into a Chroma collection"
    )

    # Add arguments
    parser.add_argument(
        "--data_directory",
        type=str,
        default="documents",
        help="The directory where your text files are stored",
    )
    parser.add_argument(
        "--collection_name",
        type=str,
        default="documents_collection",
        help="The name of the Chroma collection",
    )
    parser.add_argument(
        "--persist_directory",
        type=str,
        default="chroma_storage",
        help="The directory where you want to store the Chroma collection",
    )

    # Parse arguments
    args = parser.parse_args()

    main(
        documents_directory=args.data_directory,
        collection_name=args.collection_name,
        persist_directory=args.persist_directory,
    )
