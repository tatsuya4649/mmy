from loguru import logger
from rich.logging import RichHandler


def init_log():
    import sys

    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss.SSS}</green> <level>{level}</level> <cyan>{function}</cyan>:<cyan>{line}</cyan> {message}",
        colorize=True,
        level="DEBUG",
    )
