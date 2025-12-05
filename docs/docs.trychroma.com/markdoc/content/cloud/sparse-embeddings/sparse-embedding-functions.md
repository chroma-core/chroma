---
id: sparse-embedding-functions
name: Sparse Embedding Functions
---

# Sparse Embedding Functions

|                                                                                          | Python | Typescript |
| ---------------------------------------------------------------------------------------- | ------ | ---------- |
| Chroma Cloud Splade                                                                      | `ChromaCloudSpladeEmbeddingFunction`      | [@chroma-core/chroma-cloud-splade](https://www.npmjs.com/package/@chroma-core/chroma-cloud-splade)          |
| Chroma BM25                                     | `ChromaBm25EmbeddingFunction`      | [@chroma-core/chroma-bm25](https://www.npmjs.com/package/@chroma-core/chroma-bm25)          |
| HuggingFace | `HuggingFaceSparseEmbeddingFunction` | - |

## Custom Sparse Embedding Functions

You can create your own sparse embedding function to use with Chroma; it just needs to implement `SparseEmbeddingFunction`.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
@register_sparse_embedding_function
class MySparseEmbeddingFunction(SparseEmbeddingFunction):
    def __init__(self):
        pass

    def __call__(self, input: Documents) -> SparseVectors:
        return [SparseVectors(indices=[], values=[]) for _ in input]

    @staticmethod
    def name() -> str:
        return "my-sparse-embedding-function"

    def get_config(self) -> Dict[str, Any]:
        return {}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "SparseEmbeddingFunction":
        return MySparseEmbeddingFunction()
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import {
    ChromaValueError,
    type SparseEmbeddingFunction,
    type SparseVector,
    registerSparseEmbeddingFunction,
} from "chromadb";

export interface MySparseEmbeddingFunctionConfig {
    ...
}

class MySparseEmbeddingFunction
    implements SparseEmbeddingFunction {

    constructor(args: MySparseEmbeddingFunctionConfig) {
        ...
    }

    public async generate(texts: string[]): Promise<SparseVector[]> {
        // embed the documents here
        return sparseEmbeddings;
    }

    public async generateForQueries(texts: string[]): Promise<SparseVector[]> {
        return this.generate(texts);
    }

    public static buildFromConfig(
        config: MySparseEmbeddingFunctionConfig,
    ): MySparseEmbeddingFunction {
        return new MySparseEmbeddingFunction(config);
    }

    public getConfig(): MySparseEmbeddingFunctionConfig {
        return ...;
    }

    public validateConfigUpdate(newConfig: Record<string, any>): void {
        ...
    }

    public static validateConfig(config: MySparseEmbeddingFunctionConfig): void {
        ...
    }
}
```

{% /Tab %}

{% /TabbedCodeBlock %}

We welcome contributions! If you create a sparse embedding function that you think would be useful to others, please consider [submitting a pull request](https://github.com/chroma-core/chroma).
