import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_single_distribution(
    df: pd.DataFrame,
    column: str,
    title: str = '',
    xlabel: str = '',
    ylabel: str = '',
    bins: int = 30,
    alpha: float = 0.5,
    edgecolor: str = 'black',
    range: tuple = (0, 1)
) -> None:
    counts, bin_edges = np.histogram(df[column], bins=bins, range=range)
    total = counts.sum()
    normalized_counts = counts / total
    
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    plt.figure(figsize=(8, 5))
    plt.bar(bin_centers, normalized_counts, width=bin_edges[1] - bin_edges[0],
            alpha=alpha, edgecolor=edgecolor, label="Normalized Frequency")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_overlaid_distribution(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
    column_1: str,
    column_2: str,
    title: str = '',
    xlabel: str = '',
    ylabel: str = '',
    bins: int = 30,
    alpha: float = 0.5,
    edgecolor: str = 'black',
    range: tuple = (0, 1)
) -> None:
    counts_1, bin_edges_1 = np.histogram(df_1[column_1], bins=bins, range=range)
    counts_2, bin_edges_2 = np.histogram(df_2[column_2], bins=bins, range=range)
    total_1 = counts_1.sum()
    total_2 = counts_2.sum()
    
    bin_centers_1 = (bin_edges_1[:-1] + bin_edges_1[1:]) / 2
    bin_centers_2 = (bin_edges_2[:-1] + bin_edges_2[1:]) / 2
    
    normalized_counts_1 = counts_1 / total_1
    normalized_counts_2 = counts_2 / total_2
    
    plt.figure(figsize=(8, 5))
    plt.bar(bin_centers_1, normalized_counts_1, width=bin_edges_1[1] - bin_edges_1[0],
            alpha=alpha, edgecolor=edgecolor, label=column_1)
    plt.bar(bin_centers_2, normalized_counts_2, width=bin_edges_2[1] - bin_edges_2[0],
            alpha=alpha, edgecolor=edgecolor, label=column_2)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()
    

def compare_embedding_models(
    metrics_df: pd.DataFrame,
    metric: str = 'Recall@3',
    title: str = 'Recall@3 Scores by Model'
) -> None:
    plt.figure(figsize=(12, 6))

    models = metrics_df['Model'].tolist()
    x = np.arange(len(models))
    width = 0.4

    _, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, metrics_df[metric], width, label='Score', color="#327eff")

    ax.set_ylabel(metric)
    ax.set_xlabel('Model')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()