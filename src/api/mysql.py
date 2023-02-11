import ipaddress
from dataclasses import dataclass

from fastapi import FastAPI
from pydantic import BaseModel
from src.server import State

app = FastAPI()


class AddedNode(BaseModel):
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int


@dataclass
class AddedNodeResponse:
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int
    state: State


@dataclass
class EndAddedNodeResponse:
    pass


@app.post("/add/node")
async def added_node_as_unknown(node_info: AddedNode) -> AddedNodeResponse:
    return AddedNodeResponse(
        host=node_info.host,
        port=node_info.port,
        state=State.Unknown,
    )


@app.get("/update")
async def please_check_updating_cluster_state():
    return "ok"


@app.post("/end")
async def end_handling_to_add_node():
    return EndAddedNodeResponse()
