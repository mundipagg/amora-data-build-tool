from dash.testing.composite import DashComposite


def test_dash_app_page_container(amora_dash: DashComposite):
    assert amora_dash.find_element("#side-bar")
    assert amora_dash.find_element("#page-content")


def test_not_found_page(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/im-not-a-real-path",
        hook_id="page-not-found",
        wait_for_callbacks=True,
    )
