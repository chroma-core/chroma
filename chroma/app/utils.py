from os.path import getsize, isfile

# checks to make sure a sqlite db exists where we expect it
def isSQLite3(filename):
    if not isfile(filename):
        return False
    if getsize(filename) < 100:  # SQLite database file header is 100 bytes
        return False

    with open(filename, "rb") as fd:
        header = fd.read(100)

    return header[:16].decode("utf-8") == "SQLite format 3\x00"