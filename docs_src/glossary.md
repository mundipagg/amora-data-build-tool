# Glossary

`Data Model`

:   A Data model is...

`Entity`

:   An entity is...

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