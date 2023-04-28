from unittest.mock import patch

from dash import html
from dash.testing.composite import DashComposite


def test_feature_store_page(amora_dash: DashComposite):
    with patch(
        "amora.dash.pages.feature_store.card_item", return_value=html.Div()
    ) as card_item:
        amora_dash.visit_and_snapshot(
            resource_path="/feature-store",
            hook_id="feature-store-content",
            wait_for_callbacks=True,
            stay_on_page=True,
        )
        amora_dash.multiple_click("#side-bar .btn-close", clicks=1)
        assert card_item.called


def test_feature_store_page_error(amora_dash: DashComposite):
    with patch(
        "amora.dash.pages.feature_store.store.registry.list_feature_views",
        return_value={},
    ) as list_feature_views:
        amora_dash.visit_and_snapshot(
            resource_path="/feature-store",
            hook_id="feature-views-loading-error",
            wait_for_callbacks=True,
            stay_on_page=True,
        )
        amora_dash.multiple_click("#side-bar .btn-close", clicks=1)
        assert list_feature_views.called