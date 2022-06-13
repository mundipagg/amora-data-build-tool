from unittest.mock import patch

import pandas as pd
from dash.testing.composite import DashComposite

from amora.dash.app import dash_app

from tests.models.step_count_by_source import StepCountBySource


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
