import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# setup the app and database
app = Flask(__name__)

CHROMA_DATAMANAGER_DB_NAME = "chroma_datamanager.db"

db_uri = f"sqlite:///{os.getcwd()}/{CHROMA_DATAMANAGER_DB_NAME}"
app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
