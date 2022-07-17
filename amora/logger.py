import logging
import sys
import time
from datetime import timedelta
from functools import wraps
from typing import Callable

import humanize

from amora.config import settings

logger = logging.getLogger("amora")
logger.setLevel(settings.LOGGER_LOG_LEVEL)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))


def log_execution():
    """
    Wraps the execution of a function, calculates the execution time and logs the
    result in a human readable way. E.g:

    ```python
    from time import sleep

    from amora import logger


    @logger.log_execution()
    def do_something():
        sleep(3)
        return 42


    do_something()
    ```

    Would result in the log
    ```
    Function call to `a_module.do_something` took 3 seconds
    ```

    """

    def wrapper(fn: Callable):
        @wraps(fn)
        def decorator(*args, **kwargs):
            t0 = time.perf_counter()

            result = fn(*args, **kwargs)

            execution_delta = timedelta(seconds=time.perf_counter() - t0)
            f_name = f"{fn.__module__}.{fn.__qualname__}"
            delta = humanize.naturaldelta(execution_delta, minimum_unit="milliseconds")

            logger.debug(f"Function call to `{f_name}` took {delta}")

            return result

        return decorator

    return wrapper
