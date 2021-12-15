from setuptools import setup

setup(
    name="amora",
    version="0.1.5rc3",
    description="Amora Data Build Tool",
    url="http://github.com/mundipagg/amora-data-build-tool",
    author="TREX Data",
    author_email="diogo.martins@stone.com.br",
    license="MIT",
    packages=["."],
    entry_points={
        "console_scripts": ["amora=cli:main"],
        "pytest11": ["amora = amora.tests.pytest_plugin"],
    },
    install_requires=[
        "matplotlib~=3.4.2",
        "networkx~=2.6.3",
        "numpy~=1.21.1",
        "pandas~=1.3.0",
        "pytest~=6.2.5",
        "pytest-xdist[psutil]~=2.4.0",
        "sqlalchemy-bigquery~=1.2.0",
        "sqlmodel~=0.0.4",
        "typer[all]~=0.4.0",
        "Jinja2~=2.11.3",
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
)
