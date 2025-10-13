# Sparse Index Module

## Overview

The sparse index module implements the **Block-Max WAND (Weak AND)** algorithm for efficient sparse vector search. This implementation is built on top of Chroma's blockfile abstraction and provides high-performance top-k retrieval for sparse vectors, commonly used in text search and information retrieval systems.

## Algorithm and Query Processing

This implementation combines the WAND (Weak AND) algorithm from Broder et al. [1] with the Block-Max optimization from Ding and Suel [2] to achieve efficient sparse vector search.

### Background: WAND Algorithm

The WAND algorithm [1] introduces a two-level query evaluation process:
- **First level**: Approximate evaluation using partial information and upper bounds
- **Second level**: Full evaluation of promising candidates

The core innovation is the WAND predicate, which generalizes AND and OR operations using weighted thresholds. For a set of terms with weights w₁, w₂, ..., wₖ and threshold θ, WAND returns true when the sum of weights for present terms exceeds θ.

### Block-Max Enhancement

The Block-Max WAND algorithm [2] improves upon WAND by:
- Partitioning posting lists into fixed-size blocks (e.g., 64-256 documents)
- Storing the maximum impact score for each block
- Using block-level upper bounds instead of global maximums for more aggressive pruning

This creates a piecewise upper-bound approximation that significantly reduces the number of documents that need full evaluation.

### Query Processing Flow

The algorithm maintains cursors for each query dimension and processes documents as follows:

1. **Initialization**: Create a cursor for each query dimension tracking:
   - Current document position
   - Current block boundaries and block maximum score
   - Dimension-level maximum score (global upper bound)

2. **Pivot Selection** (following WAND):
   - Sort cursors by current document offset
   - Find pivot term: first term where accumulated upper bounds exceed threshold
   - Pivot document is the smallest document ID that could be a candidate

3. **Block-Level Optimization** (Block-Max enhancement):
   - Use block maximum scores for tighter bounds
   - Perform shallow pointer movements to check block boundaries
   - Skip entire blocks when their maximum scores cannot contribute to top-k

4. **Document Evaluation**:
   - If pivot document contains sufficient terms with high enough scores, evaluate it
   - Maintain a min-heap of top-k results
   - Update threshold based on minimum score in heap

5. **Cursor Advancement**:
   - Move cursors past evaluated or skipped documents
   - When block check fails, skip to next block boundary (not just next document)

### Performance Characteristics

According to the original papers:

- **WAND [1]** achieves 90%+ reduction in full evaluations compared to exhaustive search
- **Block-Max WAND [2]** provides 2-3x additional speedup over WAND
- **Time Complexity**: O(n) worst case, but typically sublinear due to pruning
- **Space Overhead**: ~5% increase for storing block maximum values
- **Block Size Trade-offs**:
  - Smaller blocks (64): Better pruning, more metadata overhead
  - Larger blocks (256): Less metadata, coarser pruning
  - Typical sweet spot: 128 documents per block

The combination of WAND's pivot-based traversal with Block-Max's fine-grained upper bounds makes this implementation particularly effective for high-dimensional sparse vector search.

### References

[1] Broder, A. Z., Carmel, D., Herscovici, M., Soffer, A., & Zien, J. (2003). Efficient query evaluation using a two-level retrieval process. In Proceedings of CIKM '03.

[2] Ding, S., & Suel, T. (2011). Faster top-k document retrieval using block-max indexes. In Proceedings of SIGIR '11.

## Architecture

The module consists of three main components:

### 1. Types (`types.rs`)
- Utility functions for encoding/decoding dimension IDs as base64 strings
- Constants for special prefixes used in blockfile storage

### 2. Writer (`writer.rs`)
- **SparseDelta**: Accumulates changes (creates/deletes) to sparse vectors
- **SparseWriter**: Manages the writing process with support for incremental updates
- **SparseFlusher**: Handles the final commit and flush operations

### 3. Reader (`reader.rs`)
- **SparseReader**: Provides read access to the sparse index
- **Cursor**: Internal structure for tracking position in each dimension's posting list
- **Score**: Result structure containing document offset and similarity score

## Data Layout on Blockfile

The sparse index uses two separate blockfiles to store its data. Both blockfiles follow the standard Chroma blockfile format:
```
Prefix (String) -> Key (K) -> Value (V)
```

### 1. Max Blockfile (`sparse_max`)

Stores block-level and dimension-level maximum values for efficient pruning.

**Format:**
```
Prefix: String -> Key: u32 -> Value: f32
```

**Entries:**

a) **Dimension-level maximums:**
   - Prefix: `"DIMENSION"`
   - Key: `dimension_id` (u32)
   - Value: `dimension_max_value` (f32)
   - Purpose: Stores the global maximum value for each dimension

b) **Block-level maximums:**
   - Prefix: `encode_u32(dimension_id)` (base64-encoded dimension ID)
   - Key: `block_boundary_offset` (u32)
   - Value: `block_max_value` (f32)
   - Purpose: Stores the maximum value within each block for a dimension
   - The `block_boundary_offset` is the exclusive upper bound of the block (i.e., one past the last document in the block)

### 2. Offset-Value Blockfile (`sparse_offset_value`)

Stores the actual sparse vector values for each document.

**Format:**
```
Prefix: String -> Key: u32 -> Value: f32
```

**Entries:**
- Prefix: `encode_u32(dimension_id)` (base64-encoded dimension ID)
- Key: `document_offset` (u32)
- Value: `value` (f32)
- Purpose: Stores the actual sparse vector values for each (dimension, document) pair
- Sorted by document offset within each dimension for efficient range queries

## Data Organization Example

For a sparse vector at document offset 42 with values `{dim_5: 0.8, dim_10: 0.3}` and block size 128:

**Max Blockfile entries:**
```
Prefix: "DIMENSION", Key: 5, Value: 0.9         # Global max for dimension 5
Prefix: "DIMENSION", Key: 10, Value: 0.7        # Global max for dimension 10
Prefix: encode_u32(5), Key: 128, Value: 0.9     # Block [0,128) max for dim 5
Prefix: encode_u32(10), Key: 128, Value: 0.7    # Block [0,128) max for dim 10
```

**Offset-Value Blockfile entries:**
```
Prefix: encode_u32(5), Key: 42, Value: 0.8      # Value for dim 5 at doc 42
Prefix: encode_u32(10), Key: 42, Value: 0.3     # Value for dim 10 at doc 42
```

## Usage Example

```rust
// Writing sparse vectors
let mut delta = SparseDelta::default();
delta.create(doc_offset, vec![(dim1, val1), (dim2, val2)]);
sparse_writer.write(delta).await;
let flusher = sparse_writer.commit().await?;
flusher.flush().await?;

// Querying with WAND
let query = vec![(dim1, query_val1), (dim2, query_val2)];
let top_k_results = sparse_reader.wand(query, k).await?;
```

## Implementation Notes

- Dimension IDs are encoded as base64 strings for use as blockfile keys
- The implementation supports incremental updates through the fork mechanism
- Cursors are maintained in sorted order by document offset for efficient processing
- The algorithm uses a min-heap to maintain top-k results efficiently
- Tie-breaking is handled deterministically by document offset