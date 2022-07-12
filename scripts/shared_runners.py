import os
from chroma.cli.multi_command import SubCommand

def app_subcommand(base_dir, multicommand):
    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "chroma/app"))

    subcommand = SubCommand(
        multicommand,
        name="App",
        command=["uvicorn app:app --reload --host '::'"],
        env=app_env,
        cwd=app_directory,
        ready_string="Application startup complete",
    )
    return subcommand

def redis_subcommand(base_dir, multicommand):
    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "chroma/app"))

    subcommand = SubCommand(
        multicommand,
        name="Redis",
        command=["redis-server"],
        env=app_env,
        cwd=app_directory,
        ready_string="Ready to accept connections",
    )
    return subcommand

def celery_subcommand(base_dir, multicommand):
    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "chroma/app"))

    subcommand = SubCommand(
        multicommand,
        name="Celery",
        command=["celery -A tasks.celery worker --loglevel=info"],
        env=app_env,
        cwd=app_directory,
        ready_string="Connected to redis://127.0.0.1:6379/0",
    )
    return subcommand

def frontend_subcommand(base_dir, multicommand):
    app_frontend_env = os.environ.copy()
    app_frontend_directory = "/".join((base_dir, "chroma-ui"))

    subcommand = SubCommand(
        multicommand,
        name="Frontend",
        command=["yarn start"],
        env=app_frontend_env,
        cwd=app_frontend_directory,
        ready_string="No issues found",
    )
    return subcommand
