import json

import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
from transformers import AutoTokenizer, AutoModel
import torch

class CodeBERTEmbeddingFunction(EmbeddingFunction):
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base")
        self.model = AutoModel.from_pretrained("microsoft/codebert-base")

    def __call__(self, inputs: Documents) -> Embeddings:
        output = []
        for input in inputs:
            code_tokens=self.tokenizer.tokenize(input)
            code_tokens = self._clean_input(code_tokens)
            tokens=self._format_tokens([], code_tokens)
            tokens_ids=self.tokenizer.convert_tokens_to_ids(tokens)
            context_embeddings = self.model(torch.tensor(tokens_ids)[None,:])[0]
            cls_embedding = context_embeddings[0, 0, :]
            output.append(cls_embedding.detach().cpu().numpy())
        return output

    def _clean_input(self, tokens, max_length=512-4):
        tokens = tokens[:max_length]
        return tokens

    def _format_tokens(self, nl_tokens, code_tokens):
        tokens=[self.tokenizer.cls_token]+nl_tokens+[self.tokenizer.sep_token]+code_tokens+[self.tokenizer.eos_token]
        return tokens

client = chromadb.PersistentClient()

try:
    client.delete_collection(name="code")
except:
    pass

code_collection = client.get_or_create_collection(
    name="code",
    embedding_function=CodeBERTEmbeddingFunction()
)

for lang in ['go', 'java', 'javascript', 'php', 'python', 'ruby']:
    file_path = f'data/CodeSearchNet/{lang}/test.jsonl'
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            if i >= 1000:
                break
            json_obj = json.loads(line)
            code = json_obj.pop('original_string')
            id = json_obj.pop('sha')
            json_obj = {k: json_obj[k] for k in ['repo', 'path', 'func_name', 'language', 'code', 'docstring', 'url', 'partition'] if k in json_obj}
            code_collection.add(
                documents=[code],
                metadatas=[json_obj],
                ids=[id]
            )
