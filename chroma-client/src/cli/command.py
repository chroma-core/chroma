from typing import Optional
import typer
from chroma_client import Chroma
from rich.console import Console
from rich.table import Table
from rich import box
from rich.prompt import Prompt


console = Console()

typer_app = typer.Typer()

def connect() -> Chroma:
    try:
        chroma = Chroma()
    except:
        console.print(f"Error")
        raise typer.Exit(1)
    return chroma

@typer_app.command()
def list():
    '''List all model spaces'''
    chroma = connect()
    
    data = chroma.get_model_spaces()

    if not isinstance(data, Exception):
        table = Table("Model Space", "Records", "Datasets", "Dimensionality", box=box.SIMPLE)
        for item in data:
            datasets_comma_separated = ", ".join(data[item]['datasets'])
            table.add_row(item, f"{data[item]['count']:,}".replace(",", "_"), datasets_comma_separated, str(data[item]['dimensionality']))

        # if data is empty, print a message
        if not data:
            table.add_row("", "", "Empty")

        console.print(table)
    else:
        console.print(f"{data}")

@typer_app.command()
def inspect(model_space: str = typer.Argument(None)):
    '''Inspect a model space'''
    if model_space is None:
        typer.echo("Please provide a model space name")
    
    chroma = connect()
    inspect_res = chroma.inspect(model_space=model_space)

    if not isinstance(inspect_res, Exception):
        table = Table("Model Space", "Records", "Datasets", "Dimensionality", "Inference Classes", "Label Classes", box=box.SIMPLE)
        datasets_comma_separated = "\n".join(inspect_res['datasets'])
        inference_classes_comma_separated = ", ".join(inspect_res['inference_class'])
        label_classes_comma_separated = ", ".join(inspect_res['label_class'])
        table.add_row(
            inspect_res['model_space'], 
            f"{inspect_res['count']:,}".replace(",", "_"), 
            datasets_comma_separated, 
            str(inspect_res['dimensionality']),
            inference_classes_comma_separated,
            label_classes_comma_separated
            )
        console.print(table)
    else:
        console.print(f"{inspect_res}")

@typer_app.command()
def peak(model_space: Optional[str] = typer.Argument(None)):
    '''Peak into a model space'''
    chroma = connect()
    raw_result = chroma.peak(model_space=model_space)
    if not isinstance(raw_result, Exception):
        import pandas as pd
        df = pd.DataFrame(raw_result)
        typer.echo(df)
    else:
        console.print(f"{raw_result}")

@typer_app.command()
def delete(model_space: Optional[str] = typer.Argument(None)):
    '''Delete a model space'''
    if model_space is None:
        typer.echo("Please provide a model space name")
        raise typer.Exit()

    # get count
    chroma = connect()
    count = chroma.count(model_space=model_space)

    if not isinstance(count, Exception):
        print("Deleting model space:", model_space, "with", f"{count['count']:,}".replace(",", "_"), "records")
        delete = Prompt.ask(":boom: Are you sure you want to delete it? [bold red]This can not be undone![/bold red]")
        if not delete == "y":
            print("Not deleting")
            raise typer.Exit()
        chroma.delete(where_filter={"model_space": model_space})
    else:
        console.print(f"{count}")

# reset
@typer_app.command()
def reset():
    '''Reset the database'''
    delete = Prompt.ask(":boom: Are you sure you want to drop all data? [bold red]This can not be undone![/bold red]")
    if not delete == "y":
        print("Not deleting")
        raise typer.Exit()
    chroma = connect()
    chroma.reset()

# for being called directly
if __name__ == "__main__":
    typer_app()

# for the setup.cfg entry_point
def run():
    typer_app()
