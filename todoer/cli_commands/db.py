# import typer
# import os
# import shutil
# import pathlib

# typer_app = typer.Typer()

# from backend.api import db

# @typer_app.command()
# def setup():
#     typer.echo("Setting up db...")

#     current_dir = str(pathlib.Path(__file__).parent.parent.resolve())

#     db.create_all()

#     # this is a hack to get the db out of the root dir
#     current_location = "/".join((current_dir, 'todo.db'))
#     new_location = "/".join((current_dir, 'backend', 'todo.db'))
#     shutil.move(current_location, new_location)

#     typer.secho("Sqlite db, todo.db, has been setup", fg=typer.colors.MAGENTA)

# if __name__ == "__main__":
#     typer_app()