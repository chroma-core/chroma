import os
from os.path import isfile, getsize
from api import app, db
from flask import render_template

def isSQLite3(filename):
    print(os.getcwd())
    print(filename)
    if not isfile(filename):
        return False
    if getsize(filename) < 100: # SQLite database file header is 100 bytes
        return False

    with open(filename, 'rb') as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == 'SQLite format 3\x00'

if not isSQLite3('chroma.db'):
  db.create_all()
  print('No DB existed. Created DB.')
else:
  print('DB in place')

@app.route("/")
def my_index():
    return render_template("index.html", flask_token="Hello world")
