import ipaddress

from dacite import Config, from_dict
from fastapi.testclient import TestClient
from src.api.mysql import AddedNodeResponse, app
from src.server import State

client = TestClient(app)
_TEST_HOST = str(ipaddress.ip_address("127.0.0.1"))
_TEST_PORT = 1000


def test_added_node():
    response = client.post(
        "/add/node",
        json={
            "host": _TEST_HOST,
            "port": _TEST_PORT,
        },
    )
    assert response.is_success is True
    jdata = response.json()
    data: AddedNodeResponse = from_dict(
        data=jdata,
        data_class=AddedNodeResponse,
        config=Config(
            type_hooks={
                ipaddress.IPv4Address: ipaddress.ip_address,
                State: State,
            }
        ),
    )
    assert str(data.host) == _TEST_HOST
    assert data.port == _TEST_PORT
    assert data.state is State.Unknown


def test_update():
    response = client.get("/update")
    assert response.is_success is True


def test_end_handling_to_add_node():
    response = client.post("/end")
    assert response.is_success is True
    jdata = response.json()
    assert len(jdata) == 0
