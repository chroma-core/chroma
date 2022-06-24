import typer

from chroma.cli.webapp import typer_app as application
from chroma.cli.sdk import typer_app as sdk

typer_app = typer.Typer()
typer_app.add_typer(application, name="application")
typer_app.add_typer(sdk, name="sdk")

# for being called directly
if __name__ == "__main__":
    typer_app()

# for the setup.cfg entry_point
def run():
    typer_app()
