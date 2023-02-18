import logging

from ..const import SYSTEM_NAME


def _init_client_logger() -> logging.Logger:
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        f"[{SYSTEM_NAME} Client] %(asctime)s %(levelname)s %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


client_logger = _init_client_logger()
