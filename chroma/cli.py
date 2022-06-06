import typer

from chroma.cli_commands.webapp import typer_app as application

typer_app = typer.Typer()
typer_app.add_typer(application, name="application")

# for being called directly
if __name__ == "__main__":
    typer_app()

# for the setup.cfg entry_point
def run():
  typer_app()