# Amora Model

An Amora Model is a subclass of `amora.models.AmoraModel` . A way of expressing a [data schema](#data-schema),
the [data materialization](#model-configuration) and an optional transformation statement.
`AmoraModel` is built on top of [SQLAlchemy](https://www.sqlalchemy.org/).

## Data schema

```Python
{% include "../../examples/amora_project/models/health.py" %}
```

## Model Configuration

::: amora.models.ModelConfig

## Transformation

Data transformation is defined at the model `source() -> Compilable` classmethod.

::: amora.models.AmoraModel.source

## Dependencies

A list of Amora Models that the current model depends on

::: amora.models.AmoraModel.__depends_on__


## Source models

Tables/views that already exist on the Data Warehouse and shouldn't be managed by Amora.


```Python
{% include "../../examples/amora_project/models/health.py" %}
```

Source models are models managed outside the scope of Amora, without a `source` implementation and
no [dependencies](#dependencies). [Model configurations](model-configuration) such as [materialization type](materialization-types)
and description are optional, and used for documentation purposes only.


## Materialized models

```Python
{% include "../../examples/amora_project/models/steps_agg.py" %}
```
To-do
