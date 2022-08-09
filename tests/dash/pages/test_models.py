from unittest.mock import patch

import pandas as pd
import pytest
from dash.testing.composite import DashComposite

from tests.models.step_count_by_source import StepCountBySource


@pytest.mark.skip
def test_models_page(amora_dash: DashComposite):
    with patch(
        "amora.page.components.model_details.summarize", return_value=pd.DataFrame()
    ):
        amora_dash.visit_and_snapshot(
            resource_path="/models",
            hook_id="models-content",
            wait_for_callbacks=True,
            stay_on_page=True,
        )

        amora_dash.select_dcc_dropdown(
            "div#model-select-dropdown", value=StepCountBySource.unique_name()
        )
        amora_dash.wait_for_text_to_equal(
            selector="H4.card-title", text=StepCountBySource.unique_name()
        )
