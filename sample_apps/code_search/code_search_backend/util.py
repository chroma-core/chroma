from vars import JINA_AI_API_KEY, OPENAI_API_KEY, MAX_EMBEDDING_BATCH_SIZE, CHROMA_COLLECTION_NAME

import re

from dataclasses import dataclass

import itertools
from chromadb import Documents, EmbeddingFunction, Embeddings
import torch
from transformers.models.roberta import RobertaTokenizer, RobertaModel

import chromadb
from chromadb.api import types
from chromadb.api import models
from chromadb.utils.embedding_functions import JinaEmbeddingFunction, OpenAIEmbeddingFunction

_GLOBAL_STATE = {
    "collection_name": CHROMA_COLLECTION_NAME,
    "embedding_function": None,
}

def get_state():
    return dict(_GLOBAL_STATE)


@dataclass
class RegexFilter:
    pattern: str

    def to_filter(self) -> dict[str, str]:
        return {
            "regex": self.pattern
        }

@dataclass
class MetadataFilter:
    key: str
    value: str

    def to_filter(self) -> dict[str, str]:
        return {
            self.key: self.value
        }

@dataclass
class Query:
    filters: list[RegexFilter | MetadataFilter]
    natural_language_query: str

def parse_query(query: str) -> Query:
    filters_regex = {
        r'/(.*?)/(g)?': lambda pattern: RegexFilter(pattern),
        r'(^|\s)(language|filename|extension|in):([^\s]+)': lambda matches: MetadataFilter(matches[1], matches[2])
    }
    filters = []
    for r, f in filters_regex.items():
        matches = re.findall(r, query)
        for match in matches:
            filters.append(f(match))
        query = str(re.sub(r, ' ', query))
    return Query(filters=filters, natural_language_query=query)


device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

class CodeBERTEmbeddingFunction(EmbeddingFunction):
    def __init__(self):
        self.tokenizer = RobertaTokenizer.from_pretrained("microsoft/codebert-base")
        self.model = RobertaModel.from_pretrained("microsoft/codebert-base").to(device)
        self.model.eval()

    def __call__(self, inputs: Documents) -> Embeddings:
        all_embeddings = []
        for batch in itertools.batched(inputs, MAX_EMBEDDING_BATCH_SIZE):
            tokens = self.tokenizer(batch, return_tensors='pt', padding=True, truncation=True, max_length=512)
            tokens = {k: v.to(device) for k, v in tokens.items()}
            outputs = self.model(tokens['input_ids'], attention_mask=tokens['attention_mask'])
            cls_embeddings = outputs.last_hidden_state[:, 0, :]
            all_embeddings.extend(cls_embeddings.to("cpu").detach().numpy().tolist())
        return all_embeddings

def get_embedding_function() -> types.EmbeddingFunction:
    ef: EmbeddingFunction[Documents] | None = None
    global _GLOBAL_STATE
    if JINA_AI_API_KEY:
        ef = JinaEmbeddingFunction(
            api_key=JINA_AI_API_KEY,
            model_name="jina-embeddings-v2-base-en",
        )
        _GLOBAL_STATE['embedding_function'] = 'Jina'
    elif OPENAI_API_KEY:
        ef = OpenAIEmbeddingFunction(
            api_key=OPENAI_API_KEY,
            model_name="text-embedding-ada-002",
        )
        _GLOBAL_STATE['embedding_function'] = 'OpenAI'
    else:
        ef = CodeBERTEmbeddingFunction()
        _GLOBAL_STATE['embedding_function'] = 'CodeBERT'
    return ef

_COLLECTION_REFERENCE = None
def get_chroma_collection() -> models.Collection:
    global _COLLECTION_REFERENCE
    if _COLLECTION_REFERENCE != None:
        return _COLLECTION_REFERENCE
    client = chromadb.PersistentClient()
    ef = get_embedding_function()
    code_collection = client.get_or_create_collection(
        name=CHROMA_COLLECTION_NAME,
        embedding_function=ef
    )
    _COLLECTION_REFERENCE = code_collection
    return code_collection

if __name__ == '__main__':
    raise Exception("util.py is not meant to be run directly.")
