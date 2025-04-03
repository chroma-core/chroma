import pytrec_eval
from tqdm import tqdm
import pandas as pd
import numpy as np
from typing import Any, Dict, List
import matplotlib.pyplot as plt
import chromadb
from functions.visualize import *

# Benchmarking
def query_collection(
    collection: Any, 
    query_text: List[str], 
    query_ids: List[str], 
    query_embeddings: List[np.ndarray],
    n_results: int = 10
) -> dict:
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
            results[query_id] = {
                "query_embedding": query_embedding,
                "retrieved_corpus_ids": query_results["ids"][idx],
                "retrieved_corpus_text": query_results["documents"][idx],
                "all_scores": [1 - d for d in query_results["distances"][idx]]
            }

    return results

def get_metrics(
    qrels: Dict[str, Dict[str, int]], 
    results: Dict[str, Dict[str, float]], 
    k_values: List[int]
) -> Dict[str, Dict[str, float]]:
    recall = dict()
    precision = dict()
    map = dict()
    ndcg = dict()

    for k in k_values:
        recall[f"Recall@{k}"] = 0.0
        precision[f"P@{k}"] = 0.0
        map[f"MAP@{k}"] = 0.0
        ndcg[f"NDCG@{k}"] = 0.0

    recall_string = "recall." + ",".join([str(k) for k in k_values])
    precision_string = "P." + ",".join([str(k) for k in k_values])
    map_string = "map_cut." + ",".join([str(k) for k in k_values])
    ndcg_string = "ndcg_cut." + ",".join([str(k) for k in k_values])

    evaluator = pytrec_eval.RelevanceEvaluator(qrels, {map_string, ndcg_string, recall_string, precision_string})
    
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

    return ndcg, map, recall, precision

def evaluate(
    k_values: List[int], 
    qrels_df: pd.DataFrame, 
    results_dict: Dict[str, Dict[str, float]]
) -> Dict[str, Dict[str, float]]:
    qrels = qrels_df.groupby("query-id").apply(lambda g: dict(zip(g["corpus-id"], g["score"]))).to_dict()
    
    qrels = {
        qid: {doc_id: int(score) for doc_id, score in doc_dict.items()}
        for qid, doc_dict in qrels.items()
    }

    results = {}
    for query_id, query_data in results_dict.items():
        results[query_id] = {}
        for doc_id, score in zip(query_data['retrieved_corpus_ids'], query_data['all_scores']):
            results[query_id][doc_id] = score

    ndcg, map, recall, precision = get_metrics(
        qrels=qrels, 
        results=results, 
        k_values=k_values
    )

    final_result = {
        "NDCG": ndcg,
        "MAP": map,
        "Recall": recall,
        "Precision": precision
    }
    
    return final_result

def run_benchmark(
    query_embeddings_lookup: Dict[str, Dict[str, float]],
    collection: Any,
    qrels: pd.DataFrame,
    k_values: List[int] = [1,3,5,10]
) -> Dict[str, Dict[str, float]]:
    query_ids = list(query_embeddings_lookup.keys())
    queries = [query_embeddings_lookup[query_id]["text"] for query_id in query_ids]
    query_embeddings = [query_embeddings_lookup[query_id]["embedding"] for query_id in query_ids]

    query_results = query_collection(
        collection=collection, 
        query_text=queries, 
        query_ids=query_ids, 
        query_embeddings=query_embeddings,
        n_results=20
    )

    metrics = evaluate(
        k_values=k_values, 
        qrels_df=qrels, 
        results_dict=query_results
    )

    for _, value in metrics.items():
        for k, v in value.items():
            print(f"{k}: {v}")

    return metrics

def cosine_similarity(
    vec1: np.ndarray,
    vec2: np.ndarray
) -> float:
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

# Alinging LLM Judge
def llm_vs_human(
    llm_judgements: Dict[str, Dict[str, str]],
    human_judgements: Dict[str, Dict[str, str]],
    documents_mapping: Dict[str, str],
    criteria_labels: List[str],
    criteria_threshold: int
) -> None:
    results = {}
    aligned = []
    not_aligned = []
    met_threshold = []
    overall_alignment = 0

    for criterion in criteria_labels:
        results[criterion] = 0.0

    for key, value in llm_judgements.items():
        human_judgement = human_judgements[key]

        num_criteria_met = 0

        for criterion in criteria_labels:
            if value[criterion] and human_judgement:
                results[criterion] += 1
                num_criteria_met += 1
            elif not value[criterion] and not human_judgement:
                results[criterion] += 1
                num_criteria_met += 1
        
        if num_criteria_met == len(criteria_labels):
            aligned.append(documents_mapping[key])
        elif num_criteria_met == 0:
            not_aligned.append(documents_mapping[key])

        predicted = (num_criteria_met >= criteria_threshold)
        if predicted:
            met_threshold.append({
                'id': key,
                'document': documents_mapping[key]
            })

        if predicted == human_judgement:
            overall_alignment += 1

    for criterion in criteria_labels:
        results[criterion] = results[criterion] / len(llm_judgements)

    print(results)

    overall_alignment_score = (overall_alignment / len(llm_judgements)) * 100

    print(f"Documents aligned with Human Judgement: {overall_alignment}, {overall_alignment_score}%")
    print("\n")
    print(f"Number of documents meeting threshold: {len(met_threshold)}")
    print(f"Number of documents 100% aligned: {len(aligned)}")
    print(f"Number of documents 0% aligned: {len(not_aligned)}")

    for aligned_doc in aligned[:5]:
        print(f"Aligned: {aligned_doc}")
        print("\n")

    for not_aligned_doc in not_aligned[:5]:
        print(f"Not aligned: {not_aligned_doc}")
        print("\n")

def score_query_query(
    qrels: pd.DataFrame, 
    query_embeddings_1: dict, 
    query_embeddings_2: dict, 
    column_name: str,
    output_path: str = None
) -> pd.DataFrame:
    similarity_scores = []

    # Check if inputs are lists (from evaluate_and_visualize_wandb)
    using_lists = isinstance(query_embeddings_1, list) and isinstance(query_embeddings_2, list)

    for idx, row in qrels.iterrows():
        query_id = row['query-id']
        
        if using_lists:
            # Assume the lists are in the same order as the qrels DataFrame
            col1_embedding = query_embeddings_1[idx]
            col2_embedding = query_embeddings_2[idx]
        else:
            # Use the dictionary lookup
            col1_embedding = query_embeddings_1[query_id]['embedding']
            col2_embedding = query_embeddings_2[query_id]['embedding']
        
        similarity = cosine_similarity(col1_embedding, col2_embedding).item()
        similarity_scores.append(similarity)

    scores_df = qrels.copy()
    scores_df[column_name] = similarity_scores

    if output_path:
        scores_df.to_csv(output_path)
        
    return scores_df


def score_query_document(
    qrels: pd.DataFrame, 
    query_embeddings_dict: dict, 
    corpus_embeddings_dict: dict, 
    column_name: str,
    output_path: str = None
) -> pd.DataFrame:
    similarity_scores = []

    for _, row in qrels.iterrows():
        query_id = row['query-id']
        corpus_id = row['corpus-id']
        
        query_embedding = query_embeddings_dict[query_id]['embedding']
        corpus_embedding = corpus_embeddings_dict[corpus_id]['embedding']
        
        similarity = cosine_similarity(query_embedding, corpus_embedding).item()
        similarity_scores.append(similarity)

    scores_df = qrels.copy()
    scores_df[column_name] = similarity_scores

    if output_path:
        scores_df.to_parquet(output_path)
        
    return scores_df

def evaluate_and_visualize(
    ground_truth_query_dict: Dict[str, Dict[str, float]],
    generated_query_dict: Dict[str, Dict[str, float]],
    corpus_embeddings_dict: Dict[str, Dict[str, float]],
    qrels: pd.DataFrame, 
    collection: Any,
    dataset_name: str, 
    model_name: str,
    k_values: List[int] = [1,3,5,10]
) -> Dict[str, Dict[str, float]]:
    
    query_ids = list(ground_truth_query_dict.keys())

    ground_truth_queries = [ground_truth_query_dict[query_id]["text"] for query_id in query_ids]
    generated_queries = [generated_query_dict[query_id]["text"] for query_id in query_ids]

    ground_truth_query_embeddings = [ground_truth_query_dict[query_id]["embedding"] for query_id in query_ids]
    generated_query_embeddings = [generated_query_dict[query_id]["embedding"] for query_id in query_ids]

    ground_truth_query_results = query_collection(
        collection=collection, 
        query_text=ground_truth_queries, 
        query_ids=query_ids, 
        query_embeddings=ground_truth_query_embeddings
    )
    generated_query_results = query_collection(
        collection=collection, 
        query_text=generated_queries, 
        query_ids=query_ids, 
        query_embeddings=generated_query_embeddings
    )
    
    ground_truth_query_metrics = evaluate(
        k_values=k_values, 
        qrels_df=qrels, 
        results_dict=ground_truth_query_results
    )
    generated_query_metrics = evaluate(
        k_values=k_values, 
        qrels_df=qrels, 
        results_dict=generated_query_results
    )

    print(f"Ground Truth Query Metrics:")
    print(ground_truth_query_metrics)
    print(f"\nGenerated Query Metrics:")
    print(generated_query_metrics)
    
    ground_truth_document_scores = score_query_document(
        qrels=qrels, 
        query_embeddings_dict=ground_truth_query_dict, 
        corpus_embeddings_dict=corpus_embeddings_dict,
        column_name="ground-truth-document"
    )
    
    generated_document_scores = score_query_document(
        qrels=qrels, 
        query_embeddings_dict=generated_query_dict, 
        corpus_embeddings_dict=corpus_embeddings_dict,
        column_name="generated-document"
    )
    
    plot_overlaid_distribution(
        df_1=ground_truth_document_scores,
        df_2=generated_document_scores,
        column_1="ground-truth-document",
        column_2="generated-document",
        title=f"{dataset_name} - {model_name} (Query <> Document)",
        xlabel="Cosine Similarity",
        ylabel="Normalized Frequency"
    )
    
    return {
        "ground_truth_metrics": ground_truth_query_metrics,
        "generated_metrics": generated_query_metrics
    }