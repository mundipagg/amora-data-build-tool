import logging
import sys

from amora.config import settings

logger = logging.getLogger("amora")
logger.setLevel(settings.LOGGER_LOG_LEVEL)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
