import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from chroma.app_backend.utils import fetch_datapoints

# setup the app and database
template_dir = os.path.abspath("chroma-ui/build")
static_dir = os.path.abspath("chroma-ui/build/static")
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

CHROMA_APP_DB_NAME = "chroma_app.db"

db_uri = f"sqlite:///{os.getcwd()}/{CHROMA_APP_DB_NAME}"
app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Create a local dataset from what's stored in the data manager 
_datapoints = fetch_datapoints()