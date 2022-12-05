from concurrent.futures import ThreadPoolExecutor

from amora.config import settings as amora_settings
from amora.dash.config import settings
from amora.logger import log_execution, logger
from amora.meta_queries import summarize
from amora.models import list_models
from amora.providers.bigquery import sample
from amora.questions import QUESTIONS


@log_execution()
def before_startup():
    if not amora_settings.STORAGE_CACHE_ENABLED:
        logger.debug("Cache disabled. Skipping cache generation.")
        return

    with ThreadPoolExecutor(
        max_workers=settings.THREAD_POOL_EXECUTOR_WORKERS
    ) as executor:
        # cache model summary
        for model, path_ in list_models():
            executor.submit(summarize, model)

        # cache model data sample
        for model, path_ in list_models():
            executor.submit(sample, model)

        # cache data questions
        for question in QUESTIONS:
            executor.submit(question.answer_df)
