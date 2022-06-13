from dash.testing.composite import DashComposite

from amora.dash.app import dash_app


def test_environment_page(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    dash_duo.visit_and_snapshot(
        resource_path="/environment",
        hook_id="environment-table",
        wait_for_callbacks=True,
    )
