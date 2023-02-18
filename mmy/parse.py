import ipaddress
from dataclasses import dataclass
from typing import Any, TypeAlias

import yaml
from dacite import Config, from_dict

from .etcd import MmyEtcdInfo
from .mysql.client import MmyMySQLInfo


@dataclass
class MmyYAML:
    mysql: MmyMySQLInfo
    etcd: MmyEtcdInfo


def _parse_yaml(obj: dict[str, Any]):
    config = Config(
        type_hooks={
            ipaddress.IPv4Address: ipaddress.ip_address,
            ipaddress.IPv6Address: ipaddress.ip_address,
        },
    )
    mmy: MmyYAML = from_dict(
        data_class=MmyYAML,
        data=obj,
        config=config,
    )
    return mmy


def parse_yaml(pathname: str) -> MmyYAML:
    with open(pathname, "r") as file:
        obj = yaml.safe_load(file)
        result: MmyYAML = _parse_yaml(obj)
        return result
