# Feature View

### Entities

- Used to identify the __primary key__ on which feature values are stored and retrieved. 
- Used during the lookup of feature values from the [Online Store](./feature-store.md#storage) and the join process in point-in-time joins.
- Are generally recognizable, concrete concepts or abstract, such as a customer email, a company identifier or a [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key).
- Act as primary keys. They are used during the lookup of features from the online store, and they are also used to match feature rows across feature views during point-in-time joins.

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



--8<-- "docs_src/abbreviations.md"