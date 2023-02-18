import logging

_ch_formatter = logging.Formatter(
    "%(levelname)s %(name)s - %(message)s",
)
# Stream
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(_ch_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(ch)
