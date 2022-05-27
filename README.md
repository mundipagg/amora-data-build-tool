# Amora Data Build Tool (ADBT)

[![codecov](https://codecov.io/gh/mundipagg/amora-data-build-tool/branch/main/graph/badge.svg?token=NXCHI3026S)](https://codecov.io/gh/mundipagg/amora-data-build-tool)
[![CodeFactor](https://www.codefactor.io/repository/github/mundipagg/amora-data-build-tool/badge)](https://www.codefactor.io/repository/github/mundipagg/amora-data-build-tool)
![GitHub contributors](https://img.shields.io/github/contributors/mundipagg/amora-data-build-tool?style=flat-square)
[![GitHub forks](https://img.shields.io/github/forks/mundipagg/amora-data-build-tool?style=flat-square)](https://github.com/mundipagg/amora-data-build-tool/network)
[![Stargazers](https://img.shields.io/github/stars/mundipagg/amora-data-build-tool?style=flat-square)](https://github.com/mundipagg/amora-data-build-tool/stargazers)
[![Issues](https://img.shields.io/github/issues/mundipagg/amora-data-build-tool?style=flat-square)](https://github.com/mundipagg/amora-data-build-tool/issues)
[![MIT License](https://img.shields.io/github/license/mundipagg/amora-data-build-tool?style=flat-square)](https://github.com/mundipagg/amora-data-build-tool/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![PyPI version](https://badge.fury.io/py/amora.svg)](https://badge.fury.io/py/amora)

<div align="center">
    <a href="https://www.github.com/mundipagg/amora-data-build-tool">
        <img src="https://repository-images.githubusercontent.com/411297222/d1562f7f-cef8-471d-a0c9-82624800908c" alt="Logo" />
    </a>
</div>

## About The Project

 **Amora Data Build Tool** enables analysts and engineers to transform data on the data warehouse (BigQuery) 
by writing *Amora Models* that describe the data schema using Python's [PEP484 - Type Hints](https://www.python.org/dev/peps/pep-0484/) 
and select statements with [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy). Amora is able to transform Python 
code into SQL data transformation jobs that run inside the warehouse.

## Built With

* [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy)
* [SQLModel](https://github.com/tiangolo/sqlmodel)
* [Feast](https://github.com/feast-dev/feast)
* [pytest](https://github.com/pytest-dev/pytest)
* [pytest-xdist](https://github.com/pytest-dev/pytest-xdist)

## Getting Started
### Documentation

[https://mundipagg.github.io/amora-data-build-tool](https://mundipagg.github.io/amora-data-build-tool)

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.