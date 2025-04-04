import pytrec_eval
from tqdm import tqdm
import pandas as pd
import numpy as np
from typing import Any, Dict, List
import matplotlib.pyplot as plt
from chromadb import Collection
from functions.visualize import *
from functions.types import *

# Benchmarking
def query_collection(
    collection: Collection, 
    query_text: List[str], 
    query_ids: List[str], 
    query_embeddings: List[List[float]],
    n_results: int = 10
) -> QueryResults:
    BATCH_SIZE = 100
    results = dict()

    for i in tqdm(range(0, len(query_embeddings), BATCH_SIZE), desc="Processing batches"):
        batch_text = query_text[i:i + BATCH_SIZE]
        batch_ids = query_ids[i:i + BATCH_SIZE]
        batch_embeddings = query_embeddings[i:i + BATCH_SIZE]

        query_results = collection.query(
            query_embeddings=batch_embeddings,
            query_texts=batch_text,
            n_results=n_results
        )

        for idx, (query_id, query_embedding) in enumerate(zip(batch_ids, batch_embeddings)):
            results[query_id] = QueryResultItem(
                query_embedding=query_embedding,
                retrieved_corpus_ids=query_results["ids"][idx],
                retrieved_corpus_text=query_results["documents"][idx],
                all_scores=[1 - d for d in query_results["distances"][idx]]
            )

    return QueryResults(
        doc_scores=results
    )

def get_metrics(
    qrels: QueryRelevance, 
    results: Dict[str, Dict[str, float]], 
    k_values: List[int]
) -> ResultMetrics:
    recall = dict()
    precision = dict()
    map = dict()
    ndcg = dict()
    qrels_relevances = qrels.doc_relevances

    for k in k_values:
        recall[f"Recall@{k}"] = 0.0
        precision[f"P@{k}"] = 0.0
        map[f"MAP@{k}"] = 0.0
        ndcg[f"NDCG@{k}"] = 0.0

    recall_string = "recall." + ",".join([str(k) for k in k_values])
    precision_string = "P." + ",".join([str(k) for k in k_values])
    map_string = "map_cut." + ",".join([str(k) for k in k_values])
    ndcg_string = "ndcg_cut." + ",".join([str(k) for k in k_values])

    evaluator = pytrec_eval.RelevanceEvaluator(qrels_relevances, {map_string, ndcg_string, recall_string, precision_string})
    
    scores = evaluator.evaluate(results)

    for query_id in scores.keys():
        for k in k_values:
            ndcg[f"NDCG@{k}"] += scores[query_id]["ndcg_cut_" + str(k)]
            map[f"MAP@{k}"] += scores[query_id]["map_cut_" + str(k)]
            recall[f"Recall@{k}"] += scores[query_id]["recall_" + str(k)]
            precision[f"P@{k}"] += scores[query_id]["P_"+ str(k)]
    
    for k in k_values:
        ndcg[f"NDCG@{k}"] = round(ndcg[f"NDCG@{k}"]/len(scores), 5)
        map[f"MAP@{k}"] = round(map[f"MAP@{k}"]/len(scores), 5)
        recall[f"Recall@{k}"] = round(recall[f"Recall@{k}"]/len(scores), 5)
        precision[f"P@{k}"] = round(precision[f"P@{k}"]/len(scores), 5)

    return ResultMetrics(
        ndcg=ndcg,
        map=map,
        recall=recall,
        precision=precision
    )

def evaluate(
    k_values: List[int], 
    qrels_df: pd.DataFrame, 
    results_dict: QueryResults
) -> ResultMetricsDict:
    qrels = qrels_df.groupby("query-id").apply(lambda g: dict(zip(g["corpus-id"], g["score"]))).to_dict()
    
    qrels = {
        qid: {doc_id: int(score) for doc_id, score in doc_dict.items()}
        for qid, doc_dict in qrels.items()
    }

    qrels_relevances = QueryRelevance(doc_relevances=qrels)

    results = {}
    for query_id, query_data in results_dict.doc_scores.items():
        results[query_id] = {}
        for doc_id, score in zip(query_data.retrieved_corpus_ids, query_data.all_scores):
            results[query_id][doc_id] = score

    result_metrics = get_metrics(
        qrels=qrels_relevances, 
        results=results, 
        k_values=k_values
    )

    final_result = {
        "NDCG": result_metrics.ndcg,
        "MAP": result_metrics.map,
        "Recall": result_metrics.recall,
        "Precision": result_metrics.precision
    }

    return ResultMetricsDict(
        results=final_result
    )

def run_benchmark(
    query_embeddings_lookup: QueryLookup,
    collection: Collection,
    qrels: pd.DataFrame,
    k_values: List[int] = [1,3,5,10]
) -> ResultMetricsDict:
    query_lookup = query_embeddings_lookup.lookup
    query_ids = list(query_lookup.keys())
    queries = [query_lookup[query_id].text for query_id in query_ids]
    query_embeddings = [query_lookup[query_id].embedding for query_id in query_ids]

    query_results = query_collection(
        collection=collection, 
        query_text=queries, 
        query_ids=query_ids, 
        query_embeddings=query_embeddings,
        n_results=20
    )

    query_results_scores = QueryResults(doc_scores=query_results.doc_scores)

    result_metrics = evaluate(
        k_values=k_values, 
        qrels_df=qrels, 
        results_dict=query_results_scores
    )

    for _, value in result_metrics.results.items():
        for k, v in value.items():
            print(f"{k}: {v}")

    return result_metrics.results
