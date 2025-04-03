import os
import json
from pathlib import Path
from datetime import datetime

def save_results(results, model_name):
    """
    Save benchmark results to a JSON file with a Git-friendly filename.
    
    Args:
        results: The benchmark results to save
        model_name: The name of the model used for the benchmark
    """
    # Create a timestamp with Git-friendly characters (underscores instead of spaces, hyphens instead of colons)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create the results directory if it doesn't exist
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    
    # Prepare the data to save
    results_to_save = {
        "model": model_name,
        "results": results
    }
    
    # Save the results to a file
    file_path = results_dir / f"{timestamp}.json"
    with open(file_path, 'w') as f:
        json.dump(results_to_save, f)
    
    print(f"Results saved to {file_path}")
    return file_path

# Example usage:
# from functions.evaluate import run_benchmark
# results = run_benchmark(query_embeddings_lookup, collection, qrels)
# save_results(results, "text-embedding-3-large") 