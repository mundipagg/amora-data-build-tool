from setuptools import setup

setup(
    name="amora",
    version="0.1",
    description="Amora Data Build Tool",
    url="http://github.com/mundipagg/amora-data-build-tool",
    author="TREX Data",
    author_email="diogo.martins@stone.com.br",
    license="MIT",
    packages=["."],
    entry_points={
        "console_scripts": ["amora=amora.cli:main"],
    },
)
