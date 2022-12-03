import chroma
from chroma.server.fastapi import FastAPI
settings = chroma.config.Settings(
                                chroma_db_impl="clickhouse",
                                clickhouse_host="clickhouse",
                                clickhouse_port="9000",)
server = FastAPI(settings)
app = server.app()
