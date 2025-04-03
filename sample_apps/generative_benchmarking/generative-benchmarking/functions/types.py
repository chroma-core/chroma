from dataclasses import dataclass
from typing import Dict, List

@dataclass
class QueryRelevance:
    doc_relevances: Dict[str, Dict[str, int]]

@dataclass
class QueryResults:
    doc_scores: Dict[str, Dict[str, float]]

@dataclass
class QueryLookup:
    lookup: Dict[str, Dict[str, float]]

@dataclass
class ResultMetrics:
    ndcg: Dict[str, Dict[str, float]]
    map: Dict[str, Dict[str, float]]
    recall: Dict[str, Dict[str, float]]
    precision: Dict[str, Dict[str, float]]

@dataclass
class ResultMetricsDict:
    results: Dict[str, Dict[str, float]]
