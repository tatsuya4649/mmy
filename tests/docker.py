import ipaddress
from dataclasses import dataclass


@dataclass
class container:
    container_name: str
    service_name: str
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int

    @property
    def address_format(self):
        return f"{self.host}:{self.port}"


class DockerStartupError(RuntimeError):
    pass
