from unittest.mock import patch

from amora.feature_store import patch_usage, settings


def test_patch_usage_default_behaviour():
    with patch("feast.usage") as usage:
        patch_usage()

        assert not usage._is_enabled
        assert usage.USAGE_ENDPOINT == "https://usage.feast.dev"


def test_patch_usage_custom_tracking_endpoint():
    usage_endpoint = "https://we-wont-track-you.stone.co"

    with patch.multiple(
        settings, USAGE_ENDPOINT=usage_endpoint, USAGE_TRACKING_ENABLED=True
    ), patch("feast.usage") as usage:
        patch_usage()

        assert usage._is_enabled
        assert usage.USAGE_ENDPOINT == usage_endpoint


def test_patch_usage_with_tracking_on_default_endpoint():
    with patch.multiple(settings, USAGE_TRACKING_ENABLED=True), patch(
        "feast.usage"
    ) as usage:
        patch_usage()

        assert usage._is_enabled
        assert usage.USAGE_ENDPOINT == "https://usage.feast.dev"
