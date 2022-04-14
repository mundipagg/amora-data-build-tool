# Feature Store

In order to use Amora's feature store, you need to install it with the `feature-store` extra: 

```bash
pip install amora[feature-store]
```

Using Amora's Feature Store capabilities enables data teams to: 

- Run the necessary data pipeline to transform the data into feature values
- Easily productionize new features from Amora Models
- Store and manage feature data 
- Track feature lineage, versions and related metadata
- Serve feature data consistently for training and inference purposes
- Share and reuse features across multiple teams

## Components

Amora's Feature Store is composed by the following components:


``` mermaid
graph LR
  GCS[GCS]
  
  subgraph Feature Store    
    subgraph Storage
        OnS[Online Storage]
        OffS[Offline Storage]  
    end
    
    subgraph Serving
        O[Feature Service]
    end
    
    subgraph Registry
        FR[Feature Registry]
    end
  end
  
  OnS --> R[Redis];
  OffS --> B[Big Query];
  
  O -->|get_online_features| M[Model Serving]
  O -->|get_historical_features| X[Model Training]
  
  FR --> GCS 

```


### Storage

Feature data is stored by the Feature Store to support retrieval through __Online__ 
and __Offline__ feature serving layers.

Offline Storage is typically used to store months or years of feature data for training 
purposes. In Amora, data is stored in Big Query.

Online Storage is used to persist feature values for low-latency lookup during inference. 
It only store the latest feature values for each [entity](./feature-view.md#entitites), 
essentially modeling the current state of the world. In Amora, data is stored in Redis.


``` mermaid
graph LR
 
  subgraph Feature Store    
    subgraph Storage
        OnS[Online Storage]
        OffS[Offline Storage]  
    end
    
    subgraph Serving
        O[Feature Service]
    end
  end
  
  OnS --> R[Redis];
  OffS --> B[Big Query];
   

```


### Serving

A ML Model require a consistent view of features through training and serving.  
The definitions of features used to train a model must exactly match the features 
provided in online serving. When they don’t match, [training-serving skew](https://developers.google.com/machine-learning/guides/rules-of-ml#training-serving_skew) is 
introduced, which can cause model performance problems. 
Amora's Feature Store is able to consistently serve feature data to ML Models:
 
- During the generation of training datasets, querying the offline storage for historical feature values.
- Low-latency retrieval of the latest feature value from the online store.

``` mermaid
graph LR
 
  subgraph Feature Store    
    subgraph Storage
        OnS[Online Storage]
        OffS[Offline Storage]  
    end
    
    subgraph Serving
        O[Feature Service]
    end
  end
  
  O -->|get_online_features| M[Model Serving]
  O -->|get_historical_features| X[Model Training] 

```

### Registry
 

The registry is the single source of truth for information and metadata about 
features in a project. It is a central catalog for Data Teams and automated jobs that 
registers what kind of data is stored and how it is organized. 

In Amora, the registry is stored on [GCS](https://cloud.google.com/storage) using [Feast's Feature Registry](https://docs.feast.dev/getting-started/architecture-and-components/registry).


``` mermaid
graph LR
  GCS[GCS]
  
  subgraph Feature Store    
    subgraph Storage
        OnS[Online Storage]
        OffS[Offline Storage]  
    end
    
    subgraph Serving
        O[Feature Service]
    end
    
    subgraph Registry
        FR[Feature Registry]
    end
  end
  
  FR --> GCS 

```

--8<-- "docs_src/abbreviations.md"