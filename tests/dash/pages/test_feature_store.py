from unittest.mock import patch

import pandas as pd
import pytest
from dash.testing.composite import DashComposite


@pytest.mark.skip
def test_feature_store_page(amora_dash: DashComposite):
    with patch(
        "amora.dash.pages.feature_store.summarize", return_value=pd.DataFrame()
    ) as summarize:
        amora_dash.visit_and_snapshot(
            resource_path="/feature-store",
            hook_id="feature-store-content",
            wait_for_callbacks=True,
        )

        assert summarize.called
