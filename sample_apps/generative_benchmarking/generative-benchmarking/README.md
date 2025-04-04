# Generative Benchmarking

This repository contains code for benchmarking embedding models.

## Fixing Git-Friendly Filenames

The GitHub Actions workflow is failing due to filenames with spaces and colons in the `results` directory. To fix this issue:

1. Run the `fix_filenames.py` script to rename existing files:
   ```
   python fix_filenames.py
   ```

2. For future benchmark runs, use the `save_results.py` script instead of directly saving files:
   ```python
   from save_results import save_results
   
   # After running your benchmark
   save_results(results, "your-model-name")
   ```

3. Update any code that references these files to use the new Git-friendly filenames (with underscores instead of spaces and hyphens instead of colons).

## Example of Git-Friendly Filenames

Instead of:
```
2025-03-31 13:59:25.json
```

Use:
```
2025-03-31_13-59-25.json
```

This will ensure compatibility with Git and GitHub Actions. 