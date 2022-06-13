from unittest.mock import patch

import pandas as pd
from dash.testing.composite import DashComposite

from amora.dash.app import dash_app
from amora.questions import QUESTIONS


def test_data_questions_page(dash_duo: DashComposite):
    # patching so we don't have to run the question on bigquery
    with patch("amora.questions.Question.answer_df", return_value=pd.DataFrame()):
        dash_duo.start_server(dash_app)

        dash_duo.visit_and_snapshot(
            resource_path="/questions",
            hook_id="questions-content",
            wait_for_callbacks=True,
            stay_on_page=True,
        )

        question_elements = dash_duo.find_elements(".question-card")
        assert len(question_elements) == len(QUESTIONS)
