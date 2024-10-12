import asyncio
import sys
import uuid
from pathlib import Path

import chromadb
import xai_sdk
from pypdf import PdfReader
from langchain_text_splitters import RecursiveCharacterTextSplitter, SentenceTransformersTokenTextSplitter
from tqdm import tqdm

from chromadb.utils.embedding_functions.sentence_transformer_embedding_function import \
    SentenceTransformerEmbeddingFunction


def chunk_pdf(document_name: str) -> list[tuple[str, int]]:
  """
  Chunks a PDF document
  Args:
    document_name (str): The name of the PDF document to chunk
  Returns:
    A list of chunks and the page number they are from
  """

  file_path = f"./docs/{document_name}"
  reader = PdfReader(file_path)

  chunks_with_page_numbers = []

  character_splitter = RecursiveCharacterTextSplitter(
      separators=["\n\n", "\n", ".", " ", ""],
      chunk_size=1000,
      chunk_overlap=0)

  token_splitter = SentenceTransformersTokenTextSplitter(chunk_overlap=0,
                                                         tokens_per_chunk=256)

  for page_number, page in tqdm(enumerate(reader.pages, start=1),
                                total=len(reader.pages),
                                desc="Chunking Pages"):
    page_text = page.extract_text().strip()
    if not page_text:
      continue

    split_texts = character_splitter.split_text(page_text)
    for text in split_texts:
      token_split_texts = token_splitter.split_text(text)
      for chunk in token_split_texts:
        chunks_with_page_numbers.append((chunk, page_number))

  print()
  return chunks_with_page_numbers


def load_data(collection: chromadb.Collection) -> None:
    pdfs = [file.name for file in Path("./docs").rglob('*.pdf')]
    for file in pdfs:
        if len(collection.get(where={"document_name": file}, limit=1)["ids"]) > 0:
            continue
        chunks = chunk_pdf(file)
        collection.add(
            ids=[str(uuid.uuid4()) for _ in range(len(chunks))],
            documents=[chunk[0] for chunk in chunks],
            metadatas=[{"document_name": file, "page_number": chunk[1]} for chunk in chunks],
        )


async def main():
    chroma_client = chromadb.PersistentClient(path="./chroma_data")
    embedding_function = SentenceTransformerEmbeddingFunction()
    collection = chroma_client.get_or_create_collection(
        name="context_collection",
        embedding_function=embedding_function,
    )

    load_data(collection)

    client = xai_sdk.Client()
    conversation = client.chat.create_conversation()

    print("Enter an empty message to quit.\n")

    while True:
        user_input = input("Human: ")
        print("")

        if not user_input:
            return

        context = collection.query(query_texts=[user_input], include=["documents"], n_results=5)["documents"][0]
        prompt_context = '\n\n'.join(context)
        prompt = f"User query: {user_input}. Answer using this context:\n\n {prompt_context}"

        token_stream, _ = conversation.add_response(prompt)
        print("Grok: ", end="")
        async for token in token_stream:
            print(token, end="")
            sys.stdout.flush()
        print("\n")


if __name__ == "__main__":
    asyncio.run(main())