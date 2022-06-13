from unittest.mock import patch

import pandas as pd
from dash.testing.composite import DashComposite

from amora.dash.app import dash_app

from tests.models.step_count_by_source import StepCountBySource


def test_dash_app_page_container(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    assert dash_duo.find_element("#side-bar")
    assert dash_duo.find_element("#page-content")


def test_environment_page(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    dash_duo.visit_and_snapshot(
        resource_path="/environment",
        hook_id="environment-table",
        wait_for_callbacks=True,
    )


def test_not_found_page(dash_duo: DashComposite):
    dash_duo.start_server(dash_app)

    dash_duo.visit_and_snapshot(
        resource_path="/im-not-a-real-path",
        hook_id="page-not-found",
        wait_for_callbacks=True,
    )


def test_feature_store_page(dash_duo: DashComposite):
    with patch(
        "amora.dash.routes.feature_store.summarize", return_value=pd.DataFrame()
    ) as summarize:
        dash_duo.start_server(dash_app)

        dash_duo.visit_and_snapshot(
            resource_path="/feature-store",
            hook_id="feature-store-content",
            wait_for_callbacks=True,
        )

        assert summarize.called


def test_models_page(dash_duo: DashComposite):
    with patch(
        "amora.dash.components.model_details.summarize", return_value=pd.DataFrame()
    ):
        dash_duo.start_server(dash_app)

        dash_duo.visit_and_snapshot(
            resource_path="/models",
            hook_id="models-content",
            wait_for_callbacks=True,
            stay_on_page=True,
        )

        dash_duo.select_dcc_dropdown(
            "div#model-select-dropdown", value=StepCountBySource.unique_name
        )
        dash_duo.wait_for_text_to_equal(
            selector="H4.card-title", text=StepCountBySource.unique_name
        )
