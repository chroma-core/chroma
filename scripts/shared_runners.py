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

# def rabbit_subcommand(base_dir, multicommand):
#     app_env = os.environ.copy()
#     app_directory = "/".join((base_dir, "chroma/app"))

#     subcommand = SubCommand(
#         multicommand,
#         name="RabbitMQ",
#         command=["docker run -it -d rabbitmq:3"],
#         # command=["docker run -d --name some-rabbit -p 4369:4369 -p 5671:5671 -p 5672:5672 -p 15672:15672 rabbitmq:3"],
#         env=app_env,
#         cwd=app_directory,
#         ready_string="Application startup complete",
#     )
#     return subcommand

def celery_subcommand(base_dir, multicommand):
    app_env = os.environ.copy()
    app_directory = "/".join((base_dir, "chroma/app"))

    subcommand = SubCommand(
        multicommand,
        name="Celery",
        command=["celery -A task.celery worker --loglevel=info"],
        env=app_env,
        cwd=app_directory,
        ready_string="Connected to amqp://guest:**@127.0.0.1:5672//",
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
