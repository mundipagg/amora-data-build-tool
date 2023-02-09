from feast import FeatureStore, RepoConfig

from amora.feature_store.config import settings
from amora.feature_store.logging import patch_tqdm
from amora.feature_store.online_store import patch_online_store
from amora.feature_store.usage_tracking import patch_usage

patch_usage()
patch_tqdm()
patch_online_store()

repo_config = RepoConfig(
    registry=settings.REGISTRY,
    project="amora",
    provider=settings.PROVIDER,
    online_store={
        "type": settings.ONLINE_STORE_TYPE,
        **{
            key: value.get_secret_value()
            for key, value in settings.ONLINE_STORE_CONFIG.items()
        },
    },
    offline_store={
        "type": settings.OFFLINE_STORE_TYPE,
        **settings.OFFLINE_STORE_CONFIG,
    },
    entity_key_serialization_version=2,
)

fs = FeatureStore(config=repo_config)
