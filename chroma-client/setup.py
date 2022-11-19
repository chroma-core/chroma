from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_description() -> str:
    readme_path = Path(__file__).parent / "README.md"

    if not readme_path.exists():
        return """
        # Chroma
        """.strip()

    return readme_path.read_text(encoding="utf-8")


setup(
    name="chroma",
    author="Chroma",
    author_email="hello@chroma.com",
    license="Apache-2.0",
    description="Open source continual learning framework",
    long_description_content_type="text/markdown",
    project_urls={
        "Homepage": "https://trychroma.com",
        "GitHub": "https://github.com/chroma-core/chroma",
        "Changelog": "https://github.com/chroma-core/chroma/releases",
        "Issue Tracker": "https://github.com/chroma-core/chroma/issues",
        "Twitter": "https://twitter.com/trychroma",
    },
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    # packages=find_packages(exclude=["dagster_tests*"]),
    include_package_data=True,
    install_requires=[
        'build',
        'pytest',
        'setuptools_scm',
        'httpx',
        'pyarrow ~= 9.0',
        'requests ~= 2.28',
    ],
    extras_require={
        "in-memory": ["chroma-server"],
    },
    # entry_points={
    #     "console_scripts": [
    #         "dagster = dagster.cli:main",
    #         "dagster-daemon = dagster.daemon.cli:main",
    #     ]
    # },
)