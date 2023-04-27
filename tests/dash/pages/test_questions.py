from dash.testing.composite import DashComposite

from tests.models.step_count_by_source import how_many_data_points_where_acquired


def test_data_questions_page(amora_dash: DashComposite):
    amora_dash.visit_and_snapshot(
        resource_path="/questions",
        hook_id="questions-content",
        wait_for_callbacks=True,
        stay_on_page=True,
    )
    amora_dash.multiple_click("#side-bar .btn-close", clicks=1)
    amora_dash.select_dcc_dropdown(
        "div#questions-selector", value=how_many_data_points_where_acquired.uid
    )

    amora_dash.wait_for_text_to_equal(
        selector="H5.card-title", text=how_many_data_points_where_acquired.name
    )
