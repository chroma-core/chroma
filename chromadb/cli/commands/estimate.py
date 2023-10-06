from typing import Tuple, Union
import typer
import humanize
from rich.console import Console
from rich.table import Table

console = Console()

mem_cpu_mapping = {1: 1, 2: 1, 4: 2, 8: 4, 16: 8, 32: 16, 64: 32, 128: 64}
common_vector_dims = {1536: ["OpenAI"], 768: [
    "BERT"], 512: ["ResNet"], 368: ["all-MiniLM-L6-v2"]}

# percentage of memory used for system memory, python process (e.g. SQL queries etc)
overhead_factor = 0.3


def find_nearest_increment(value: Union[int, float], increments: list[int]) -> int:
    return min(increments, key=lambda x: x if x >= value else float('inf'))


def memory_requirement(num_vectors: int, dimensionality: int) -> int:
    mem_bytes = num_vectors * dimensionality * 4
    mem_gb = mem_bytes / (1024 ** 3)
    # Replace with your predefined increments
    increments = [2, 4, 8, 16, 32, 64]
    return find_nearest_increment(mem_gb, increments)


def max_vectors_for_memory(target_memory_gb: int, dimensionality: int) -> int:
    mem_bytes = target_memory_gb * (1024 ** 3)
    return int(mem_bytes / (dimensionality * 4))


avg_words_per_page = 250


def estimate_words(doc_count: int, page_per_doc: int, avg_words_per_page: int) -> int:
    return doc_count * page_per_doc * avg_words_per_page


def words_to_tokens(word_count: int, token_multiplier: float = 1.2) -> int:
    return int(word_count * token_multiplier)


# Example
memory = memory_requirement(1000, 200)


estimate_app = typer.Typer()


def est(docs_count: int, page_count: int, dimensionality: int, growth_factor: float) \
        -> Tuple[int, int, int, int]:
    words = estimate_words(int(docs_count), int(
        page_count), avg_words_per_page)
    tokens = int(words_to_tokens(words) * growth_factor)
    embeddings_count = int(tokens/dimensionality)
    memory = memory_requirement(embeddings_count, dimensionality)
    final_memory = memory + memory * overhead_factor
    mem_idx = find_nearest_increment(
        final_memory, list(mem_cpu_mapping.keys()))

    return tokens, embeddings_count, mem_idx, mem_cpu_mapping[mem_idx]


def baseline() -> None:
    """ Prints a table of common memory increments for Cloud providers """
    table = Table(show_header=True,
                  header_style="bold magenta", title="Estimated Max Vectors")
    table.add_column("Memory (GB)")
    table.add_column("vCPU")
    for dim in common_vector_dims.keys():
        table.add_column(
            f"{dim} Dimensions [e.g. {' '.join(common_vector_dims[dim])}]")

    for mem in mem_cpu_mapping.keys():
        cols = [f"{mem}",
                f"{mem_cpu_mapping[mem]}"]
        for dim in common_vector_dims.keys():
            max_vectors = max_vectors_for_memory(mem, dim)
            estimated_max_vectors = max_vectors - overhead_factor * max_vectors
            cols.append(
                f"{humanize.intword(estimated_max_vectors)}")
        table.add_row(
            *cols,
        )
    console.print(table)
    console.print(
        "Note: 30% memory overhead for Chroma server and OS", style="italic red")


def estimate_fixed() -> None:
    docs_count = typer.prompt("How many documents do you plan to index?")
    page_count = typer.prompt(
        "How many pages are there in these documents on average?")
    ef = typer.prompt(
        "What Embedding Function are you planning to use?"
        "[OpenAI, BERT, ResNet, all-MiniLM-L6-v2]")

    if "OpenAI" in ef.strip():
        dimensionality = 1536
    elif "BERT" in ef.strip():
        dimensionality = 768
    elif "ResNet" in ef.strip():
        dimensionality = 512
    elif "all-MiniLM-L6-v2" in ef.strip():
        dimensionality = 368
    else:
        dimensionality = 512

    _, embeddings_count, mem, cpu = est(
        docs_count, page_count, dimensionality, 1.0)
    table = Table(show_header=True,
                  header_style="bold magenta", title="Estimated Memory")
    table.add_column("Documents/Vectors (Chroma)")
    table.add_column("Memory (GB)")
    table.add_column("vCPU")
    table.add_column("Dimensions")
    table.add_column("Embedding Function")

    table.add_row(
        f"{humanize.intword(embeddings_count)}",
        f"{humanize.intword(mem)}",
        f"{cpu}",
        f"{dimensionality}",
        f"{ef}"
    )
    console.print(table)


def estimate_growth() -> None:
    # TODO: We need to start with a question what is the mental concept of the user for
    # a document - e.g. a PDF, a chapter, a page, a paragraph, a chat message etc.
    # then we align this with what a document is in chroma - max EF output size
    docs_count = typer.prompt("How many documents do have today?")
    page_count = typer.prompt(
        "How many pages are there in these documents on average?")
    ef = typer.prompt(
        "What Embedding Function are you planning to use?"
        "[OpenAI, BERT, ResNet, all-MiniLM-L6-v2]")

    growth = typer.prompt(
        "What is your expected growth rate month over month? (factor)")

    if "OpenAI" in ef.strip():
        dimensionality = 1536
    elif "BERT" in ef.strip():
        dimensionality = 768
    elif "ResNet" in ef.strip():
        dimensionality = 512
    elif "all-MiniLM-L6-v2" in ef.strip():
        dimensionality = 368
    else:
        dimensionality = 512
    table = Table(show_header=True,
                  header_style="bold magenta", title="Estimated Sizing")
    table.add_column("Timeline")
    table.add_column("Memory (GB)")
    table.add_column("vCPU")

    table.add_column("Documents/Vectors (Chroma)")

    table.add_column("Dimensions")
    table.add_column("Embedding Function")

    for m in range(0, 13, 6):
        _, embeddings_count, mem, cpu = est(
            docs_count, page_count, dimensionality, m**int(growth) if m > 0 else 1.0)
        table.add_row(
            "Now" if m == 0 else f"{m} months",
            f"{cpu}",
            f"{dimensionality}",
            f"{humanize.intword(embeddings_count)}",
            f"{humanize.intword(mem)}",
            f"{ef}"
        )

    console.print(table)


if __name__ == "__main__":
    baseline()
