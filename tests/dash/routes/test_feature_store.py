from unittest.mock import patch

import pandas as pd
from dash.testing.composite import DashComposite

from amora.dash.app import dash_app


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
