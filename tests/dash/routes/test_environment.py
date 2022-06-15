from dash.testing.composite import DashComposite


def test_environment_page(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/environment",
        hook_id="environment-table",
        wait_for_callbacks=True,
    )
