from dash.testing.composite import DashComposite


def test_feature_store_page(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/feature-store",
        hook_id="feature-store-content",
        wait_for_callbacks=True,
        stay_on_page=True,
    )
