from typing import Iterable, Tuple

from pydantic import BaseSettings


class Settings(BaseSettings):
    K8S_NAMESPACE: str = "default"
    """The namespace to deploy the operator to"""
    K8S_OPERATOR_DOCKER_IMAGE: str = "gcr.io/amora-data-build-tool/amora:latest"
    """The docker image to use for the operator"""
    K8S_OPERATOR_DOCKER_IMAGE_PULL_POLICY: str = "Always"
    """The docker image pull policy to use for the operator"""
    K8S_OPERATOR_IS_DELETE_OPERATOR_POD: bool = True
    """Whether to delete the operator pod after the task is complete"""

    K8S_OPERATOR_SERVICE_ACCOUNT_SECRET_NAME: str = "amora-k8s-operator"
    """The name of the secret containing the service account to use for the operator"""
    K8S_OPERATOR_SERVICE_ACCOUNT_SECRET_KEY: str = "service-account"
    """The key of the secret containing the service account to use for the operator"""

    K8S_OPERATOR_REDIS_CONNECTION_SECRET_NAME: str = "amora-k8s-operator"
    """The name of the secret containing the redis connection string to use for the operator"""
    K8S_OPERATOR_REDIS_CONNECTION_SECRET_KEY: str = "redis-connection-string"
    """The key of the secret containing the redis connection string to use for the operator"""

    class Config:
        env_prefix = "AMORA_AIRFLOW_"

    def k8s_env_vars(self) -> Iterable[Tuple[str, str]]:
        from amora.config import settings as base_settings
        from amora.dash.config import settings as dash_settings
        from amora.feature_store.config import settings as feature_store_settings

        for s in (base_settings, feature_store_settings, dash_settings, self):
            for key, value in s.dict().items():
                if value:
                    yield f"{s.Config.env_prefix}{key}", str(value)


settings = Settings()
