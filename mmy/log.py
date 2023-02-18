import abc
import logging
from enum import Enum

from colorama import Fore, Style, init


class ABCLog(abc.ABC):
    @abc.abstractproperty
    def logger(self):
        """
        Get your class logger
        """


class LogColor(Enum):
    WHITE = Fore.WHITE
    GREEN = Fore.GREEN
    BLUE = Fore.BLUE
    YELLOW = Fore.YELLOW
    RED = Fore.RED


DEFAULT_COLORS = {
    logging.DEBUG: LogColor.GREEN,
    logging.INFO: LogColor.BLUE,
    logging.WARNING: LogColor.YELLOW,
    logging.ERROR: LogColor.RED,
}


init(autoreset=True)


class ColorFormatter(logging.Formatter):
    def __init__(
        self,
        colors: dict[int, LogColor] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._colors = colors if colors is not None else DEFAULT_COLORS

    def formatMessage(self, record: logging.LogRecord):
        rd = record.__dict__
        levelno: int = rd["levelno"]
        levelname: str = rd["levelname"]
        rd["levelname"] = (
            self._colors[levelno].value
            + Style.BRIGHT
            + levelname
            + Style.RESET_ALL
            + Fore.RESET
        )
        return super().formatMessage(record)
