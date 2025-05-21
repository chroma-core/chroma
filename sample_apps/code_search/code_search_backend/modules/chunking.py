from dataclasses import dataclass
from flask import url_for
from tree_sitter import Language, Parser, Tree
from tree_sitter_language_pack import SupportedLanguage, get_language, get_parser

@dataclass
class CodeChunk:
    source_code: str
    repo: str
    file_path: str
    func_name: str
    language: str
    start_line: int
    url: str | None = None

def chunk_code_using_cst_approximation(code: str, language: SupportedLanguage) -> list[CodeChunk]:
    chunks = []
    lines = code.split('\n')
    for i, line in enumerate(lines):
        if line.strip().startswith('def'):
            chunks.append(CodeChunk(
                source_code=line,
                repo='repo',
                file_path='file_path',
                func_name='func_name',
                language=language,
                start_line=i + 1,
                url=url_for('chunk', chunk_id=i)
            ))
    return chunks

def chunk_code_using_tree_sitter(code: str, language: SupportedLanguage) -> list[CodeChunk]:
    parser = get_parser(language)
    tree = parser.parse(bytes(code, 'utf-8'))
    print(tree.root_node.children)
    return []

if __name__ == "__main__":
    raise Exception("chunking.py is not meant to be run directly.")
