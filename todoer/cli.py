import typer

# from cli_commands.db import typer_app as db
from todoer.cli_commands.webapp import typer_app as application
# from todoer.cli_commands.db import typer_app as db

typer_app = typer.Typer()
typer_app.add_typer(application, name="application")
# typer_app.add_typer(db, name="db")

# for being called directly
if __name__ == "__main__":
    typer_app()

# for the setup.cfg entry_point
def run():
  typer_app()