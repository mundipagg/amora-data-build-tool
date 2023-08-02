import pytest
from dash.testing.composite import DashComposite


@pytest.mark.skip("Precisa fazer setup do Registry")
def test_feature_store_page(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/feature-store",
        hook_id="feature-store-content",
        wait_for_callbacks=True,
    )


def test_feature_store_page_error(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/feature-store",
        hook_id="feature-views-loading-error",
        wait_for_callbacks=True,
        stay_on_page=True,
    )
