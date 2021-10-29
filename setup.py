import sys
from pathlib import Path

from setuptools import setup, find_packages

if sys.version_info < (3, 8):
    print("Error: Amora does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)

here = Path(__file__)
readme = here.parent.joinpath("README.md").read_text()

setup(
    name="amora",
    version="0.0.1",
    description="Amora Data Build Tool",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="http://github.com/mundipagg/amora-data-build-tool",
    author="TREX Data",
    author_email="diogo.martins@stone.com.br",
    license="MIT",
    packages=find_packages(exclude=["dbt", "assets", "tests*"]),
    entry_points={
        "console_scripts": ["amora=amora.cli:main"],
    },
    install_requires=[
        "jupyter~=1.0.0",
        "matplotlib~=3.4.2",
        "networkx[all]~=2.6.3",
        "numpy~=1.21.1",
        "pandas~=1.3.0",
        "sqlalchemy-bigquery~=1.2.0",
        "sqlmodel~=0.0.4",
        "typer[all]~=0.4.0",
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: SQL",
    ],
    python_requires=">=3.8",
)
