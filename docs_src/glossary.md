# Glossary

`Data Model`

:   A data model organizes data elements and standardizes how the data elements relate to one another. [1]

`Entity`

:   Entities are used to identify the __primary key__ on which feature values are stored and retrieved.

`Feature`

:   A feature is an individual measurable property. It is typically a property 
observed on a specific entity, but does not have to be associated with one. 
For example, a feature of a customer entity could be the number of transactions 
they have made on an average month, while a feature that is not observed on a 
specific entity could be the total number of transactions made by all customers 
in the last month.


`Training-Serving Skew`

:   Training-serving skew is a difference between performance during training and performance during serving. This skew can be caused by:

    - A discrepancy between how you handle data in the training and serving pipelines.
    - A change in the data between when you train and when you serve.
    - A feedback loop between your model and your algorithm.


1. https://cedar.princeton.edu/understanding-data/what-data-model 