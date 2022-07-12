import requests
import os

from dotenv import dotenv_values
from pathlib import Path

dotenv_path = Path('../.env')
config = dotenv_values("../.env")
print(str(config['DB_FILE_VERSION']))

def download(url: str, dest_folder: str, filename: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    # filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))

# get pre-trained model
download("https://chroma-datastore.sfo3.digitaloceanspaces.com/mnist_cnn-" + config['MODEL_FILE_VERSION'] + ".pt", dest_folder="../examples/mnist_embeddings", filename='mnist_cnn.pt')

# get pre-filled db
download("https://chroma-datastore.sfo3.digitaloceanspaces.com/chroma-" + config['DB_FILE_VERSION'] + ".db", dest_folder="../chroma/app", filename='chroma.db')