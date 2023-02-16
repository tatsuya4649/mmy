import ipaddress
from dataclasses import dataclass


@dataclass
class container:
    container_name: str
    service_name: str
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int
