import os
import re
from pathlib import Path

def fix_filenames():
    """
    Rename files in the results directory to use Git-friendly characters.
    Replaces spaces with underscores and colons with hyphens.
    """
    results_dir = Path("results")
    
    # Check if the directory exists
    if not results_dir.exists():
        print(f"Directory {results_dir} does not exist.")
        return
    
    # Get all JSON files in the directory
    json_files = list(results_dir.glob("*.json"))
    
    for file_path in json_files:
        # Extract the filename
        filename = file_path.name
        
        # Create a new filename with underscores instead of spaces and hyphens instead of colons
        new_filename = filename.replace(" ", "_").replace(":", "-")
        
        # If the filename has changed, rename the file
        if new_filename != filename:
            new_file_path = file_path.parent / new_filename
            print(f"Renaming {filename} to {new_filename}")
            file_path.rename(new_file_path)

if __name__ == "__main__":
    fix_filenames() 