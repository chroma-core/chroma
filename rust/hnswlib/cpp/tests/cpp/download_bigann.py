import os.path
import os

links = [
    "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs.gz",
    "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_gnd.tar.gz",
    "ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz",
]

os.makedirs("downloads", exist_ok=True)
os.makedirs("bigann", exist_ok=True)
for link in links:
    name = link.rsplit("/", 1)[-1]
    filename = os.path.join("downloads", name)
    if not os.path.isfile(filename):
        print("Downloading: " + filename)
        try:
            os.system("wget --output-document=" + filename + " " + link)
        except Exception as inst:
            print(inst)
            print("  Encountered unknown error. Continuing.")
    else:
        print("Already downloaded: " + filename)
    if filename.endswith(".tar.gz"):
        command = "tar -zxf " + filename + " --directory bigann"
    else:
        command = "cat " + filename + " | gzip -dc > bigann/" + name.replace(".gz", "")
    print("Unpacking file:", command)
    os.system(command)
