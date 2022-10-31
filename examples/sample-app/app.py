from flask import Flask
import chroma_client

app = Flask(__name__)


@app.route('/')
def hello():
    return(str(chroma_client.fetch_new_labels()))