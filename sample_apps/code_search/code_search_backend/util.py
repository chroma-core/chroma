from vars import MAX_EMBEDDING_BATCH_SIZE

import re

from dataclasses import dataclass

import itertools
from chromadb import Documents, EmbeddingFunction, Embeddings
import torch
from transformers.models.roberta import RobertaTokenizer, RobertaModel


@dataclass
class RegexFilter:
    pattern: str

    def to_filter(self) -> dict[str, str]:
        return {"regex": self.pattern}


@dataclass
class MetadataFilter:
    key: str
    value: str

    def to_filter(self) -> dict[str, str]:
        return {self.key: self.value}


@dataclass
class Query:
    filters: list[RegexFilter | MetadataFilter]
    natural_language_query: str


def parse_query(query: str) -> Query:
    filters_regex = {
        r"/(.*?)/(g)?": lambda pattern: RegexFilter(pattern),
        r"(^|\s)(language|filename|extension|in):([^\s]+)": lambda matches: MetadataFilter(
            matches[1], matches[2]
        ),
    }
    filters = []
    for r, f in filters_regex.items():
        matches = re.findall(r, query)
        for match in matches:
            filters.append(f(match))
        query = str(re.sub(r, " ", query))
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
            tokens = self.tokenizer(
                batch,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512,
            )
            tokens = {k: v.to(device) for k, v in tokens.items()}
            outputs = self.model(
                tokens["input_ids"], attention_mask=tokens["attention_mask"]
            )
            cls_embeddings = outputs.last_hidden_state[:, 0, :]
            all_embeddings.extend(cls_embeddings.to("cpu").detach().numpy().tolist())
        return all_embeddings


if __name__ == "__main__":
    raise Exception("util.py is not meant to be run directly.")
