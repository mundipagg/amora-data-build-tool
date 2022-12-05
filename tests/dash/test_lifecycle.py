from unittest.mock import MagicMock, call, patch

from amora.config import settings as amora_settings
from amora.dash import lifecycle
from amora.dash.config import settings
from amora.meta_queries import summarize
from amora.models import list_models
from amora.providers.bigquery import sample
from amora.questions import QUESTIONS


@patch("amora.dash.lifecycle.ThreadPoolExecutor")
def test_before_startup_with_cache_disabled(ThreadPoolExecutor: MagicMock):
    with patch.multiple(amora_settings, STORAGE_CACHE_ENABLED=False):
        lifecycle.before_startup()

    ThreadPoolExecutor.assert_not_called()


@patch("amora.dash.lifecycle.ThreadPoolExecutor")
def test_before_startup_with_cache_enabled(ThreadPoolExecutor: MagicMock):
    with patch.multiple(amora_settings, STORAGE_CACHE_ENABLED=True):
        lifecycle.before_startup()

    ThreadPoolExecutor.assert_called_once_with(
        max_workers=settings.THREAD_POOL_EXECUTOR_WORKERS
    )

    executor = ThreadPoolExecutor.return_value
    submit = executor.__enter__.return_value.submit

    submit.assert_has_calls([call(summarize, model) for model, _ in list_models()])
    submit.assert_has_calls([call(sample, model) for model, _ in list_models()])
    submit.assert_has_calls([call(question.answer_df) for question in QUESTIONS])
