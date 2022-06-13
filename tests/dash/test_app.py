from dash.testing.composite import DashComposite

from amora.dash.app import dash_app


def test_dash_app_page_container(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    assert dash_duo.find_element("#side-bar")
    assert dash_duo.find_element("#page-content")


def test_not_found_page(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    dash_duo.visit_and_snapshot(
        resource_path="/im-not-a-real-path",
        hook_id="page-not-found",
        wait_for_callbacks=True,
    )
