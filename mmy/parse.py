from dataclasses import dataclass
from typing import Any, TypeAlias

import yaml
from dacite import from_dict
from loguru import logger
from rich import print

from .mysql.client import MmyMySQLInfo


@dataclass
class MmyYAML:
    mysql: MmyMySQLInfo


def _parse_yaml(obj: dict[str, Any]):
    mmy: MmyYAML = from_dict(data_class=MmyYAML, data=obj)
    return mmy


def parse_yaml(pathname: str) -> MmyYAML:
    with open(pathname, "r") as file:
        obj = yaml.safe_load(file)
        result: MmyYAML = _parse_yaml(obj)
        return result
