import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# setup the app and database
template_dir = os.path.abspath('chroma-ui/build')
static_dir = os.path.abspath('chroma-ui/build/static')
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

db_uri = f"sqlite:///{os.getcwd()}/chroma.db"
app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
