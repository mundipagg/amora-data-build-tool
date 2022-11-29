# Amora Data Build Tool

**Amora Data Build Tool** enables analysts and engineers to transform data on the data warehouse (BigQuery) 
by writing *Amora Models* that describe the data schema using Python's [PEP484 - Type Hints](https://www.python.org/dev/peps/pep-0484/) 
and select statements with [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy). Amora is able to transform Python 
code into SQL data transformation jobs that run inside the warehouse.

## Installation

### Pypi

```shell
pip install amora
```

### Setup Big Query permissions

In order to use Amora with BigQuery, you'll need to setup a [keyfile](https://cloud.google.com/iam/docs/reference/rest/v1/projects.serviceAccounts.keys).

1. Go to the [BigQuery credential wizard](https://console.cloud.google.com/apis/credentials/wizard). Ensure that the right project is selected in the header bar.
2. Generate credentials with the following options:
    - **Which API are you using?** BigQuery API
    - **What data will you be accessing?** Application data (you'll be creating a service account)
    - **Are you planning to use this API with App Engine or Compute Engine?** No
    - **Service account name:** amora-user
    - **Role**: BigQuery Job User & BigQuery User
    - **Key type**: JSON
3. Download the JSON file and save it in an easy-to-remember spot, with a clear filename (e.g. `~/.bq-service-accounts/amora-user-credentials.json`)
4. Set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the path of the JSON file that contains your service 
   account key. You can add `export GOOGLE_APPLICATION_CREDENTIALS=~/.bq-service-accounts/amora-user-credentials.json` 
   to your shell initialization script (`.zshrc` for zsh, `.bash_profile` for bash, ...)

Required roles:
   
- BigQuery Data Editor (`roles/bigquery.dataEditor`)
- BigQuery User (`roles/bigquery.user`)

## How to use?

Amora is packed with a **compiler** and a **runner**. The **compiler** (CLI [`amora compile`][amora-compile]) compiles Amora's python 
models into SQL statements which can be executed against the configured data warehouse using [`amora materialize`][amora-materialize].

![Amora](static/demo.gif)

The animation above displays an user inside an Amora Project using Amora's CLI [`amora compile`][amora-compile] to compile model 
files into SQL statements and creating the corresponding views/tables into the Data Warehouse (BigQuery) using [`amora materialize`][amora-materialize].

If you want to know more about the features, check the [features](features) page 
