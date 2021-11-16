# Example Amora Project

> The following commands assume that your current working directory is `~/examples/amora_project`.

A demo amora project, using public health data collected using with Apple Health.


## /models

Models root folder

## /tests

Models data test folder. Every test is a pytest. The suggested convention is to keep 
a 1:1 relation with model files. 
E.g.: `models/heart_agg.py` -> `tests/test_heart_agg.py` 


## /target

Compiled SQL files

# Running

Make sure you have amora properly configured:

- `export AMORA_TARGET_PROJECT=my-gcp-project`: Which BigQuery project amora should use to deploy the models
- `export AMORA_TARGET_SCHEMA=amora`: Which BigQuery dataset amora should use to deploy the models
- `export AMORA_TARGET_PATH=~/examples/amora-project/target`
- `export AMORA_MODELS_PATH=~/examples/amora-project/models`

With amora properly configured: 

1. Run `amora compile` to compile the models in `AMORA_MODELS_PATH` into the SQL files in `AMORA_TARGET_PATH`
2. Run `amora materialize` to create the models 
defined in `AMORA_MODELS_PATH` into the `AMORA_TARGET_SCHEMA` dataset of the 
`AMORA_TARGET_PROJECT` GCP project.
3. Run `amora test` to assert that the data is created and in the expected state.