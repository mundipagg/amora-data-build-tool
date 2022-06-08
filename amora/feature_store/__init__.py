from feast import FeatureStore, RepoConfig

from amora.feature_store.config import settings
from amora.feature_store.logging import patch_tqdm
from amora.feature_store.usage_tracking import patch_usage

patch_usage()
patch_tqdm()

repo_config = RepoConfig(
    registry=settings.REGISTRY,
    project="amora",
    provider=settings.PROVIDER,
    online_store={
        "type": settings.ONLINE_STORE_TYPE,
        **settings.ONLINE_STORE_CONFIG,
    },
    offline_store={
        "type": settings.OFFLINE_STORE_TYPE,
        **settings.OFFLINE_STORE_CONFIG,
    },
    repo_path=settings.REPO_PATH,
)

fs = FeatureStore(config=repo_config)
