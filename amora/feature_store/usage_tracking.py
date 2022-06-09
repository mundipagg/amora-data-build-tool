import os

from amora.feature_store.config import settings


def patch_usage() -> None:
    """
    By default, feast tracks user activity. We believe that should be an opt-in feature, instead of opt-out.

    Reference: https://docs.feast.dev/reference/usage
    """
    from feast import usage
    from feast.constants import FEAST_USAGE

    os.environ[FEAST_USAGE] = str(settings.USAGE_TRACKING_ENABLED)

    usage._is_enabled = settings.USAGE_TRACKING_ENABLED
    usage.USAGE_ENDPOINT = settings.USAGE_ENDPOINT
