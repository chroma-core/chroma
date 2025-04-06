# Generative Benchmarking

This project provides a comprehensive toolkit for generating custom benchmarks and replicating the results outlined in our [technical report](https://research.trychroma.com/generative-benchmarking).

## Motivation

Benchmarking is used to evaluate how well a model is performing, with the aim to generalize that performance to broader real-world scenarios. However, the widely-used benchmarks today often rely on artificially clean datasets and generic domains, with the added concern that they have likely already been seen by embedding models in training.

We introduce generative benchmarking as a way to address these limitations. Given a set of documents, we synthetically generate queries that are representative of the ground truth.


## Overview
This repository offers tools to:
- **Generate Custom Benchmarks:** Generate benchmarks tailored to your data and use case
- **Compare Results:** Compare metrics from your generated benchmark

## Repository Structure

- **`generate_benchmark.ipynb`**  
  A comprehensive guide to generating a custom benchmark based on your data

- **`compare.ipynb`**  
  A framework for comparing results, which is useful when evaluating different embedding models or configurations

- **`data/`**  
  Example data to immediately test out the notebooks with

- **`functions/`**  
  Functions used to run notebooks, includes various embedding functions and llm prompts

- **`results/`**  
  Folder for saving benchmark results, includes results produced from example data



## Installation

### pip

```bash
pip install -r requirements.txt
```

### poetry
```bash
poetry install
```

### conda
```bash
conda env create -f environment.yml
conda activate generative-benchmarking-env
```