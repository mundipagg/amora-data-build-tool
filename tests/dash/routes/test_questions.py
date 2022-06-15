from unittest.mock import patch

import pandas as pd
from dash.testing.composite import DashComposite

from amora.questions import QUESTIONS


def test_data_questions_page(amora_dash: DashComposite):
    # patching so we don't have to run the question on bigquery
    with patch("amora.questions.Question.answer_df", return_value=pd.DataFrame()):
        amora_dash.visit_and_snapshot(
            resource_path="/questions",
            hook_id="questions-content",
            wait_for_callbacks=True,
            stay_on_page=True,
        )

        question_elements = amora_dash.find_elements(".question-card")
        assert len(question_elements) == len(QUESTIONS)
