import pandas as pd
from typing import List, Dict

def combined_datasets_dataframes(
    queries: pd.DataFrame, 
    corpus: pd.DataFrame, 
    qrels: pd.DataFrame
) -> pd.DataFrame:
    qrels = qrels.merge(queries, left_on="query-id", right_on="_id", how="left")
    qrels.rename(columns={"text": "query-text"}, inplace=True)
    qrels.drop(columns=["_id"], inplace=True)
    qrels = qrels.merge(corpus, left_on="corpus-id", right_on="_id", how="left")
    qrels.rename(columns={"text": "corpus-text"}, inplace=True)
    qrels.drop(columns=["_id", "title"], inplace=True)

    return qrels

def create_metrics_dataframe(results_list: List[Dict[str, Dict[str, float]]]) -> pd.DataFrame:
    all_metrics = []

    for result in results_list:
        model = result["model"]
        results = result["results"]

        all_metrics.append((model, results))
        
    rows = []

    for model, metrics in all_metrics:
        row = {
            'Model': model,
            'Recall@1': metrics['Recall']['Recall@1'],
            'Recall@3': metrics['Recall']['Recall@3'],
            'Recall@5': metrics['Recall']['Recall@5'],
            'Recall@10': metrics['Recall']['Recall@10'],
            'Precision@3': metrics['Precision']['P@3'],
            'Precision@5': metrics['Precision']['P@5'],
            'Precision@10': metrics['Precision']['P@10'],
            'NDCG@3': metrics['NDCG']['NDCG@3'], 
            'NDCG@5': metrics['NDCG']['NDCG@5'],
            'NDCG@10': metrics['NDCG']['NDCG@10'],
            'MAP@3': metrics['MAP']['MAP@3'],
            'MAP@5': metrics['MAP']['MAP@5'], 
            'MAP@10': metrics['MAP']['MAP@10'],
        }
        rows.append(row)

    metrics_df = pd.DataFrame(rows)

    return metrics_df