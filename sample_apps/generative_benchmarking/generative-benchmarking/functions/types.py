from dataclasses import dataclass
from typing import Dict, List

@dataclass
class QueryRelevance:
    doc_relevances: Dict[str, Dict[str, int]]

@dataclass
class QueryResultItem:
    query_embedding: List[float]
    retrieved_corpus_ids: List[str]
    retrieved_corpus_text: List[str]
    all_scores: List[float]

@dataclass
class QueryResults:
    doc_scores: Dict[str, QueryResultItem]

@dataclass
class QueryItem:
    text: str
    embedding: List[float]

@dataclass
class QueryLookup:
    lookup: Dict[str, QueryItem]

@dataclass
class ResultMetrics:
    ndcg: Dict[str, Dict[str, float]]
    map: Dict[str, Dict[str, float]]
    recall: Dict[str, Dict[str, float]]
    precision: Dict[str, Dict[str, float]]

@dataclass
class ResultMetricsDict:
    results: Dict[str, Dict[str, float]]
