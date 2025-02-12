import os
import shutil

from sys import platform
from pydriller import Repository


speedtest_src_path = os.path.join("tests", "python", "speedtest.py")
speedtest_copy_path = os.path.join("tests", "python", "speedtest2.py")
shutil.copyfile(
    speedtest_src_path, speedtest_copy_path
)  # the file has to be outside of git

commits = list(Repository(".", from_tag="v0.6.2").traverse_commits())
print("Found commits:")
for idx, commit in enumerate(commits):
    name = commit.msg.replace("\n", " ").replace("\r", " ")
    print(idx, commit.hash, name)

for commit in commits:
    name = commit.msg.replace("\n", " ").replace("\r", " ").replace(",", ";")
    print("\nProcessing", commit.hash, name)

    if os.path.exists("build"):
        shutil.rmtree("build")
    os.system(f"git checkout {commit.hash}")

    # Checking we have actually switched the branch:
    current_commit = list(Repository(".").traverse_commits())[-1]
    if current_commit.hash != commit.hash:
        print("git checkout failed!!!!")
        print("git checkout failed!!!!")
        print("git checkout failed!!!!")
        print("git checkout failed!!!!")
        continue

    print("\n\n--------------------\n\n")
    ret = os.system("python -m pip install .")
    print("Install result:", ret)

    if ret != 0:
        print("build failed!!!!")
        print("build failed!!!!")
        print("build failed!!!!")
        print("build failed!!!!")
        continue

    # os.system(f'python {speedtest_copy_path} -n "{hash[:4]}_{name}" -d 32 -t 1')
    os.system(f'python {speedtest_copy_path} -n "{commit.hash[:4]}_{name}" -d 16 -t 1')
    os.system(f'python {speedtest_copy_path} -n "{commit.hash[:4]}_{name}" -d 16 -t 64')
    # os.system(f'python {speedtest_copy_path} -n "{name}" -d 64 -t 1')
    # os.system(f'python {speedtest_copy_path} -n "{name}" -d 128 -t 1')
    # os.system(f'python {speedtest_copy_path} -n "{name}" -d 4 -t 24')
    # os.system(f'python {speedtest_copy_path} -n "{name}" -d 128 -t 24')
