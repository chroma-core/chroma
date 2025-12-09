import argparse
import os
from typing import List

#import google.generativeai as genai
from google import genai
import chromadb
from chromadb.utils import embedding_functions

#model = genai.GenerativeModel('gemini-2.5-flash') #("gemini-2.0-flash-lite") # ("gemini-pro")
GENAI_MODEL = 'gemini-2.5-flash'
GENAI_EMBED_MODEL = 'gemini-embedding-001'


def build_prompt(query: str, context: List[str]) -> str:
    """
    Builds a prompt for the LLM. #

    This function builds a prompt for the LLM. It takes the original query,
    and the returned context, and asks the model to answer the question based only
    on what's in the context, not what's in its weights.

    Args:
    query (str): The original query.
    context (List[str]): The context of the query, returned by embedding search.

    Returns:
    A prompt for the LLM (str).
    """

    base_prompt = {
        "content": "I am going to ask you a question, which I would like you to answer"
        " based only on the provided context, and not any other information."
        " If there is not enough information in the context to answer the question,"
        ' say "I am not sure", then try to make a guess.'
        " Break your answer up into nicely readable paragraphs.",
    }
    user_prompt = {
        "content": f" The question is '{query}'. Here is all the context you have:"
        f'{(" ").join(context)}',
    }

    # combine the prompts to output a single prompt string
    system = f"{base_prompt['content']} {user_prompt['content']}"

    return system


def get_gemini_response(genai_client, query: str, context: List[str]) -> str:
    """
    Queries the Gemini API to get a response to the question.

    Args:
    query (str): The original query.
    context (List[str]): The context of the query, returned by embedding search.

    Returns:
    A response to the question.
    """

    response = genai_client.models.generate_content(model=GENAI_MODEL, contents=build_prompt(query, context))

    return response.text


def main(
    collection_name: str = "documents_collection", persist_directory: str = "."
) -> None:
    # Check if the GOOGLE_API_KEY environment variable is set. Prompt the user to set it if not.
    if "GOOGLE_API_KEY" not in os.environ:
        gapikey = input("Please enter your Google API Key: ")
        os.environ['GOOGLE_API_KEY'] = gapikey

    # Instantiate a persistent chroma client in the persist_directory.
    # This will automatically load any previously saved collections.
    # Learn more at docs.trychroma.com
    chroma_client = chromadb.PersistentClient(path=persist_directory)

    # create embedding function
    #embedding_function = embedding_functions.GoogleGenerativeAiEmbeddingFunction(
    #    api_key=google_api_key, task_type="RETRIEVAL_QUERY"
    #)
    embedding_function = embedding_functions.GoogleGenaiEmbeddingFunction(model_name=GENAI_EMBED_MODEL)

    # Get the collection.
    collection = chroma_client.get_collection(
        name=collection_name, embedding_function=embedding_function
    )

    # Create Google genai client
    genai_client = embedding_function.client #genai.Client(api_key=google_api_key)

    # We use a simple input loop.
    while True:
        # Get the user's query
        query = input("Query: ")
        if len(query) == 0:
            print("Please enter a question. Ctrl+C to Quit.\n")
            continue
        print("\nThinking...\n")

        # Query the collection to get the 5 most relevant results
        results = collection.query(
            query_texts=[query], n_results=5, include=["documents", "metadatas"]
        )

        sources = "\n".join(
            [
                f"{result['filename']}: line {result['line_number']}"
                for result in results["metadatas"][0]  # type: ignore
            ]
        )
        document_data = "\n".join(results["documents"][0]) 
        print(f"Most relevant source documents from chroma collection:\n{sources}\n")
        print(f"Most relevant source document_data:\n{document_data}\n")
        #print(f"Collection Query Results:\n{results}")

        # Get the response from Gemini
        response = get_gemini_response(genai_client, query, results["documents"][0])  # type: ignore
        print(f"Gemini Response:\n{response}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load documents from a directory into a Chroma collection"
    )

    parser.add_argument(
        "--persist_directory",
        type=str,
        default="chroma_storage",
        help="The directory where you want to store the Chroma collection",
    )
    parser.add_argument(
        "--collection_name",
        type=str,
        default="documents_collection",
        help="The name of the Chroma collection",
    )

    # Parse arguments
    args = parser.parse_args()

    main(
        collection_name=args.collection_name,
        persist_directory=args.persist_directory,
    )
