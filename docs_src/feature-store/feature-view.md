# Feature View

### Entities

Entities are used to identify the __primary key__ on which feature values are stored 
and retrieved. They are used as keys during the lookup of feature values from the 
[Online Store](./feature-store.md#storage) and the join process in point-in-time joins.
Entities are generally recognizable, concrete concepts or abstract, such as a customer 
email, a document number or a [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key).

It is possible to define multiple entities for a single a feature view and is also possible to have zero entities.

A dataset with a `customer_id` entity:

| event_timestamp     | customer_id | sum_amount_spent_last_30d | 
| ------------------- | ----------- | ------------------------- |
| 2022-04-04T01:00:00 | 1           | 1000                      |
| 2022-04-04T01:00:00 | 2           | 5500                      |
| 2022-04-04T01:00:00 | 3           | 2000                      |
| 2022-04-05T01:00:00 | 1           | 2000                      |

A dataset with multiple entities, `customer_id` and `company_id`:

| event_timestamp     | customer_id | company_id  | sum_amount_spent_last_30d | 
| ------------------- | ----------- | ----------- | ------------------------- |
| 2022-04-04T01:00:00 | 1           | a           | 400                       |
| 2022-04-04T01:00:00 | 1           | b           | 600                       |
| 2022-04-04T01:00:00 | 2           | a           | 5000                      |
| 2022-04-04T01:00:00 | 2           | b           | 500                       |
| 2022-04-04T01:00:00 | 3           | a           | 2000                      |
| 2022-04-05T01:00:00 | 1           | a           | 2000                      |

### Features

A feature is an individual measurable property. It is typically a property observed on 
a specific entity, but does not have to be associated with an entity. 

A feature of a customer entity could be the number of transactions they have made on  
a month, `count_transactions_last_30d`:

| event_timestamp     | customer_id | count_transactions_last_30d | 
| ------------------- | ----------- | --------------------------- |
| 2022-04-01T01:00:00 | 1           | 10                          |
| 2022-04-01T01:00:00 | 2           | 3                           |
| 2022-04-01T01:00:00 | 3           | 5                           |
| 2022-05-01T01:00:00 | 1           | 10                          |
| 2022-05-01T01:00:00 | 2           | 20                          |
| 2022-05-01T01:00:00 | 3           | 30                          |

A feature unrelated to an entity could be the number of transactions made by all 
customers in the last month, `count_all_transactions_last_30d`:

| event_timestamp     | count_all_transactions_last_30d | 
| ------------------- | ------------------------------- |
| 2022-04-01T01:00:00 | 18                              |
| 2022-05-01T01:00:00 | 60                              |


### Feature View

A Feature View aggregates entities, features and a data source, allowing the 
Feature Store to consistently manage feature data across time. 

!!! info

    Read more on [Feast's documentation](https://docs.feast.dev/getting-started/concepts/feature-view).

On Amora, defining a Feature View from an `AmoraModel` is done by decorating the model with 
`amora.feature_store.decorators.feature_view` and implementing the [protocol](https://peps.python.org/pep-0544/) 
[`FeatureViewProtocol`](feature-store/feature-view-protocol.md). 

E.g: `StepCountBySource` is a data model that exposes the features `value_avg`, 
`value_sum` and `value_count` of each `source_name` entity on a given `event_timestamp`.

```Python
{% include "../../examples/amora_project/models/step_count_by_source.py" %}
```


--8<-- "docs_src/abbreviations.md"