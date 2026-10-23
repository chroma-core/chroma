from anthropic import Anthropic as AnthropicClient
from openai import OpenAI as OpenAIClient
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request
from typing import Dict, List
from tqdm import tqdm
import pandas as pd
import requests
import re

# Document Filtering
def filter_documents(
    client: OpenAIClient,
    model: str,
    documents: List[str],
    ids: List[str],
    criteria: List[str],
    criteria_labels: List[str]
) -> List[str]:
        
    SYSTEM_INSTRUCTION = """
        You are an assistant specialized in filtering documents based on specific criteria.

        Given a document and a criterion, evaluate whether the document meets the criterion and output a single word: "yes" if the document meets the criterion, or "no" if it does not. Do not include any extra text or formatting, simply "yes" or "no".
        """
    
    labels = {}
    filtered_document_ids = []

    for document, id in tqdm(zip(documents, ids), total=len(documents), desc="Filtering documents"):
        labels[id] = {}

        for criterion, criterion_label in zip(criteria, criteria_labels):
            PROMPT = f"""
                Evaluate the following document with the criterion below.

                Criterion: {criterion}

                Document: {document}

                Output a single word: "yes" if the document meets the criterion, or "no" if it does not. Do not include any extra text or formatting, simply "yes" or "no".
                """
            
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_INSTRUCTION},
                    {"role": "user", "content": PROMPT}
                ]
            )

            if completion.choices[0].message.content == "yes":
                labels[id][criterion_label] = True
            else:
                labels[id][criterion_label] = False
        
        passed_all = True
        
        for criterion_label in criteria_labels:
            if not labels[id][criterion_label]:
                passed_all = False
                break

        if passed_all:
            filtered_document_ids.append(id)

    return filtered_document_ids

def create_document_filter_batch(
    client: AnthropicClient,
    documents: List[str],
    ids: List[str],
    criteria: List[str],
    criteria_labels: List[str]
) -> str:
        
    SYSTEM_INSTRUCTION = """
        You are an assistant specialized in filtering documents based on specific criteria.

        Given a document and a criterion, evaluate whether the document meets the criterion and output a single word: "yes" if the document meets the criterion, or "no" if it does not. Do not include any extra text or formatting, simply "yes" or "no".
        """
    
    requests = []

    for document, id in zip(documents, ids):
        for criterion, criterion_label in zip(criteria, criteria_labels):
            request_id = f"{id}_{criterion_label}"

            PROMPT = f"""
                Evaluate the following document with the criterion below.

                Criterion: {criterion}

                Document: {document}

                Output a single word: "yes" if the document meets the criterion, or "no" if it does not. Do not include any extra text or formatting, simply "yes" or "no".
                """
                
            requests.append(Request(
                custom_id=request_id,
                params=MessageCreateParamsNonStreaming(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=8192,
                    temperature=0.2,
                    system=SYSTEM_INSTRUCTION,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": PROMPT
                                }
                            ]
                        }
                    ]
                )
            ))
    
    batch = client.messages.batches.create(requests=requests)

    print(f"Batch (id: {batch.id}) created successfully")

    return batch.id

def retrieve_document_filter_batch(
    client: AnthropicClient,
    batch_id: str
) -> Dict[str, Dict[str, str]]:
    batch = client.messages.batches.results(batch_id)
    
    results = {}

    for item in batch:
        id = item.custom_id.split("_")[0]
        criterion = item.custom_id.split("_")[1]

        if id not in results:
            results[id] = {}

        if item.result.message.content[0].text == "yes":
            results[id][criterion] = True
        else:
            results[id][criterion] = False

    return results

def retrieve_document_filter_batch_df(
    client: AnthropicClient, 
    batch_id: str
) -> Dict[str, Dict[str, str]]:
    batch = client.messages.batches.results(batch_id)

    ids = []
    criteria = []
    classification = []

    for item in batch:
        id = item.custom_id.split("_")[0]
        criterion = item.custom_id.split("_")[1]

        ids.append(id)
        criteria.append(criterion)
        if item.result.message.content[0].text == "yes":
            classification.append(True)
        else:
            classification.append(False)

    result_df = pd.DataFrame({"id": ids, "criterion": criteria, "classification": classification})

    return result_df

def get_filtered_ids(
    filtered_documents_batch_df: pd.DataFrame,
) -> List[str]:
    grouped = filtered_documents_batch_df.groupby('id')
    filtered_ids = grouped.filter(lambda x: x['classification'].all()).id.unique()
    
    return filtered_ids

# Query Generation
def create_golden_dataset(
    client: OpenAIClient, 
    model: str,
    documents: List[str], 
    ids: List[str],
    context: str,
    example_queries: str
) -> pd.DataFrame:
    
    if len(ids) != len(documents):
        raise ValueError("Length of ids must match length of documents")
    
    queries = []
    
    SYSTEM_INSTRUCTION = f"""
        You are an assistant specialized in generating queries to curate a high-quality synthetic dataset.

        Simply output the query without any additional words or formatting.
        """

    for id, document in tqdm(zip(ids, documents), total=len(ids), desc="Generating queries"):
        PROMPT = f"""
            Consider the context: 
            {context}

            Based on the following piece of text:
            <text>
            {document}
            <text>

            Please generate a realistic query that a user may ask relevant to the information provided above.

            Here are some example queries that users have asked which you should consider when generating your query:
            <example-queries>
            {example_queries}
            <example-queries>

            Do not repeat the example queries, they are only provided to give you an idea of the type of queries that users ask. 
            Make your query relevant to the information provided above and keep it in a similar style to the example queries, which may not always be in a complete question format.
            
            Simply output the query without any additional words.
            """

        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user", "content": PROMPT}
            ]
        )

        queries.append(completion.choices[0].message.content)

    queries_df = pd.DataFrame({"id": ids, "query": queries})

    return queries_df

def create_golden_dataset_batch(
    client: AnthropicClient, 
    model: str,
    documents: List[str], 
    ids: List[str],
    context: str,
    example_queries: str
) -> str:
    
    if len(ids) != len(documents):
        raise ValueError("Length of ids must match length of documents")
    
    SYSTEM_INSTRUCTION = f"""
        You are an assistant specialized in generating queries to curate a high-quality synthetic dataset.

        Simply output the query without any additional words or formatting.
        """
    
    requests = []

    for id, document in zip(ids, documents):
        PROMPT = f"""
            Consider the context: 
            {context}

            Based on the following piece of text:
            <text>
            {document}
            <text>

            Please generate a realistic query that a user may ask relevant to the information provided above.

            Here are some example queries that users have asked which you should consider when generating your query:
            <example-queries>
            {example_queries}
            <example-queries>

            Do not repeat the example queries, they are only provided to give you an idea of the type of queries that users ask. 
            Make your query relevant to the information provided above and keep it in a similar style to the example queries, which may not always be in a complete question format.
            
            Simply output the query without any additional words in this format:
            <format>
            [query]
            <format>
            """

        requests.append(Request(
            custom_id=id,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=8192,
                temperature=1,
                system=SYSTEM_INSTRUCTION,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": PROMPT
                            }
                        ]
                    }
                ]
            )
        ))

    batch = client.messages.batches.create(requests=requests)

    print(f"Batch (id: {batch.id}) created successfully")

    return batch.id


def retrieve_batch(
    client: AnthropicClient, 
    batch_id: str
) -> pd.DataFrame:
    batch = client.messages.batches.results(batch_id)

    ids = []
    queries = []

    for item in batch:
        ids.append(item.custom_id)
        queries.append(item.result.message.content[0].text)

    result_df = pd.DataFrame({"id": ids, "query": queries})

    return result_df

# Results Replication
def create_naive_query_batch(
    client: AnthropicClient,
    model: str,
    documents: List[str],
    ids: List[str]
) -> str:
    if len(ids) != len(documents):
        raise ValueError("Length of ids must match length of documents")
    
    SYSTEM_INSTRUCTION = "You are an assistant specialized in generating queries to curate a high-quality synthetic dataset"
    
    requests = []
    for id, document in zip(ids, documents):
        PROMPT = f"""
            Based on the following piece of information:
            <text>
            {document}
            <text>

            Please generate a query relevant to the information provided above.

            Simply output the query without any additional words in this format:
            <format>
            [query]
            <format>
            """
        
        requests.append(Request(
            custom_id=id,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=8192,
                temperature=1,
                system=SYSTEM_INSTRUCTION,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": PROMPT
                            }
                        ]
                    }
                ]
            )
        ))
    
    batch = client.messages.batches.create(requests=requests)

    print(f"Batch (id: {batch.id}) created successfully")

    return batch.id

def create_naive_query_multilingual_batch(
    client: AnthropicClient,
    model: str,
    documents: List[str],
    ids: List[str],
    language: str
) -> str:
    
    if len(ids) != len(documents):
        raise ValueError("Length of ids must match length of documents")
    
    SYSTEM_INSTRUCTION = f"You are an assistant specialized in generating queries to curate a high-quality synthetic dataset in {language}"
    
    requests = []
    for id, document in zip(ids, documents):
        PROMPT = f"""
            Based on the following piece of information:
            <text>
            {document}
            <text>

            Please generate a query relevant to the information provided above in {language}.

            Simply output the query without any additional words in this format:
            <format>
            [query]
            <format>
            """
        
        requests.append(Request(
            custom_id=id,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=8192,
                temperature=1,
                system=SYSTEM_INSTRUCTION,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": PROMPT
                            }
                        ]
                    }
                ]
            )
        ))
    
    batch = client.messages.batches.create(requests=requests)

    print(f"Batch (id: {batch.id}) created successfully")

    return batch.id
    
def create_distinct_query_batch(
    client: AnthropicClient,
    model: str,
    documents: List[str],
    ids: List[str],
    queries: List[str]
) -> str:
    if len(ids) != len(documents):
        raise ValueError("Length of ids must match length of documents")
    
    SYSTEM_INSTRUCTION = "You are an assistant specialized in generating queries to curate a high-quality synthetic dataset"
    
    requests = []
    for id, document, query in zip(ids, documents, queries):
        PROMPT = f"""
            Based on the following information:
            <text>
            {document}
            <text>

            This would be an example query that would be good for this kind of context:
            <query>
            {query}
            <query>

            Please generate one additional query that is distinct from the example, but is still relevant to the corpus. This point is very important, ensure that the generated query does not repeat the given example query.

            Simply output the query without any additional words in this format:
            <format>
            [query]
            <format>
            """

        requests.append(Request(
            custom_id=id,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=8192,
                temperature=1,
                system=SYSTEM_INSTRUCTION,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": PROMPT
                            }
                        ]
                    }
                ]
            )
        ))
    
    batch = client.messages.batches.create(requests=requests)

    print(f"Batch (id: {batch.id}) created successfully")

    return batch.id

def clean_id_for_batching(id_str: str) -> str:
    cleaned = re.sub(r'[^a-zA-Z0-9_-]', '_', str(id_str))
    return cleaned

def revert_id_from_batching(id_str: str) -> str:
    reverted = re.sub(r'_', '.', str(id_str), count=1)
    return reverted