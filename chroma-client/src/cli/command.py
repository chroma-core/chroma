from typing import Optional
import typer
from chroma_client import Chroma


typer_app = typer.Typer()

@typer_app.command()
def hello(name: Optional[str] = None):
    if name:
        typer.echo(f"Hello {name}")
    else:
        typer.echo("Hello World!")

@typer_app.command()
def count(model_space: Optional[str] = typer.Argument(None)):
    chroma = Chroma()
    typer.echo(chroma.count(model_space=model_space))

# for being called directly
if __name__ == "__main__":
    typer_app()

# for the setup.cfg entry_point
def run():
    typer_app()
