import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from python_on_whales import docker
from rich import print

from mmy.mysql.hosts import MySQLHostBroken, MySQLHosts
from mmy.mysql.proxy import ProxyServer
from mmy.mysql.proxy_connection import ProxyConnection, proxy_connect
from mmy.mysql.proxy_err import MmyLocalInfileUnsupportError, MmyUnmatchServerError
from mmy.server import Server, State, _Server

from .conftest import TEST_TABLE1
from .test_mysql import DockerMySQL, container

HOST = "127.0.0.1"
PROXY_SERVER_STARTUP_TIMEOUT: int = 10


@pytest.fixture
def mysql_hosts():
    _mh = MySQLHosts()
    _servers: list[Server] = list()
    for i in DockerMySQL.__members__.values():
        _cont = i.value
        _servers.append(
            Server(
                host=_cont.host,
                port=_cont.port,
                state=State.Run,
            )
        )
    _mh.update(_servers)
    return _mh


@asynccontextmanager
async def run_proxy_server(
    mysql_hosts: MySQLHosts,
    unused_tcp_port: int,
):
    ps = ProxyServer(
        mysql_hosts=mysql_hosts,
        host=HOST,
        port=unused_tcp_port,
        connection_timeout=1,
        command_timeout=1,
    )
    task = asyncio.create_task(ps.serve())
    try:
        # Waiting for start up server
        async def _ping_proxy_server():
            while True:
                try:
                    _, writer = await asyncio.open_connection(
                        HOST,
                        unused_tcp_port,
                    )
                    writer.close()
                    await writer.wait_closed()
                    return
                except ConnectionRefusedError:
                    await asyncio.sleep(0.05)
                    continue

        async def _timeout_ping():
            await asyncio.wait_for(
                _ping_proxy_server(),
                timeout=PROXY_SERVER_STARTUP_TIMEOUT,
            )

        await _timeout_ping()
        yield ps
    finally:
        await ps.shutdown()
        task.cancel()


import pytest_asyncio


@pytest_asyncio.fixture()
async def proxy_server_start(mysql_hosts, unused_tcp_port: int):
    async with run_proxy_server(
        mysql_hosts=mysql_hosts,
        unused_tcp_port=unused_tcp_port,
    ):
        yield unused_tcp_port

    return


def gen_private_key():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    return private_key


def gen_csr(private_key) -> x509.CertificateSigningRequest:
    builder = x509.CertificateSigningRequestBuilder()
    builder = builder.subject_name(
        x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "client")])
    )
    request: x509.CertificateSigningRequest = builder.sign(
        private_key=private_key,
        algorithm=hashes.SHA256(),
    )
    return request


import tempfile


def copy_ca_from_docker() -> tuple[x509.Certificate, Any]:

    mmy1 = DockerMySQL.MySQL1.value

    MYSQL_DRI = "/var/lib/mysql"
    _CA_FILENAME = "/ca.pem"
    _CA_KEY_FILENAME = "/ca-key.pem"
    CA_PATH = MYSQL_DRI + _CA_FILENAME
    CA_KEY_PATH = MYSQL_DRI + _CA_KEY_FILENAME

    with tempfile.TemporaryDirectory() as dirname:
        docker.copy((mmy1.container_name, CA_PATH), dirname)
        with open(dirname + _CA_FILENAME, "rb") as f:
            _pem_data = f.read()
            ca: x509.Certificate = x509.load_pem_x509_certificate(_pem_data)

        docker.copy((mmy1.container_name, CA_KEY_PATH), dirname)
        with open(dirname + _CA_KEY_FILENAME, "rb") as f:
            _pem_data = f.read()
            ca_key = serialization.load_pem_private_key(
                data=_pem_data,
                password=None,
            )

        return (ca, ca_key)


@dataclass
class ClientSSLInfo:
    private_key_filepath: str
    certificate_filepath: str


@pytest.fixture
def create_client_certificate():
    ca_cert, ca_key = copy_ca_from_docker()
    client_private_key = gen_private_key()
    client_csr: x509.CertificateSigningRequest = gen_csr(private_key=client_private_key)
    CLIENT_PRIVATE_KEY_FILENAME: str = "client-key.pem"
    CLIENT_CERTIFICATE_FILENAME: str = "client-cert.pem"
    with tempfile.TemporaryDirectory() as dirname:
        now = datetime.utcnow()
        new_subject = x509.Name(
            [
                x509.NameAttribute(x509.NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(x509.NameOID.STATE_OR_PROVINCE_NAME, "Texas"),
                x509.NameAttribute(x509.NameOID.LOCALITY_NAME, "Austin"),
                x509.NameAttribute(x509.NameOID.ORGANIZATION_NAME, "New Org Name!"),
            ]
        )
        builder = (
            x509.CertificateBuilder()
            .issuer_name(ca_cert.issuer)
            .serial_number(x509.random_serial_number())
            .public_key(client_csr.public_key())
            .subject_name(new_subject)
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=36500))
            .sign(
                private_key=ca_key,
                algorithm=hashes.SHA256(),
            )
        )
        client_private_key_filepath = os.path.join(dirname, CLIENT_PRIVATE_KEY_FILENAME)
        client_certificate_filepath = os.path.join(dirname, CLIENT_CERTIFICATE_FILENAME)
        with open(client_private_key_filepath, "wb") as f:
            pem = client_private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
            f.write(pem)

        with open(client_certificate_filepath, "wb") as f:
            f.write(builder.public_bytes(serialization.Encoding.PEM))

        assert os.path.isfile(client_private_key_filepath)
        assert os.path.isfile(client_certificate_filepath)
        yield ClientSSLInfo(
            private_key_filepath=client_private_key_filepath,
            certificate_filepath=client_certificate_filepath,
        )


def fix_proxy_connect(key: str, port: int):
    _client: ProxyConnection = proxy_connect(
        key=key,
        host=HOST,
        port=port,
        db="test",
        user="root",
        password="root",
        connect_timeout=0.1,
        local_infile=True,
    )
    return _client


@pytest.mark.asyncio
async def test_proxy(
    proxy_server_start,
):
    port = proxy_server_start
    random_key = str(time.time_ns())
    async with await fix_proxy_connect(random_key, port) as connect:
        await connect.ping()
        print("Helo?")


@pytest.mark.asyncio
async def test_SSL(
    proxy_server_start,
    create_client_certificate: ClientSSLInfo,
):
    import ssl

    ctx = ssl.create_default_context()
    ctx.load_cert_chain(
        certfile=create_client_certificate.certificate_filepath,
        keyfile=create_client_certificate.private_key_filepath,
    )
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.options |= ssl.OP_NO_SSLv2
    ctx.options |= ssl.OP_NO_SSLv3

    random_key = str(time.time_ns())
    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        await connect.ping()


@pytest.mark.asyncio
async def test_select(
    proxy_server_start,
):
    random_key = str(time.time_ns())

    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            length = 100
            await cursor.execute(
                key=random_key,
                query="SELECT * FROM %s LIMIT %d" % (TEST_TABLE1, length),
            )
            res = await cursor.fetchall()
            assert isinstance(res, list)
            assert len(res) == length


@pytest.mark.asyncio
async def test_insert(
    proxy_server_start,
):
    random_key = str(time.time_ns())

    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            res = await cursor.execute(
                key=random_key,
                query="INSERT INTO %s (name) VALUES (%s)" % (TEST_TABLE1, random_key),
            )
            assert res == 1

        await connect.commit()


@pytest.mark.asyncio
async def test_update(
    proxy_server_start,
):
    random_key = int(time.time_ns())
    random_id = random.randint(100000, 200000)

    async with fix_proxy_connect(str(random_key), proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            res = await cursor.execute(
                key=str(random_id),
                query="INSERT INTO %s (id, name) VALUES (%d, %s)"
                % (TEST_TABLE1, random_id, str(random_key)),
            )
            assert res == 1

            new_name = str(time.time_ns())
            res = await cursor.execute(
                key=str(random_key),
                query="UPDATE %s SET name=%s WHERE id=%d"
                % (TEST_TABLE1, new_name, random_id),
            )
            assert res == 1

        await connect.commit()


@pytest.mark.asyncio
async def test_delete(
    proxy_server_start,
):
    random_key = str(time.time_ns())

    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            res = await cursor.execute(
                key=random_key,
                query="INSERT INTO %s (name) VALUES (%s)" % (TEST_TABLE1, random_key),
            )
            assert res == 1

        await connect.commit()

        cursor = await connect.cursor()
        async with cursor:
            res = await cursor.execute(
                key=random_key,
                query="DELETE FROM %s WHERE name=%s" % (TEST_TABLE1, random_key),
            )
            assert res == 1

        await connect.commit()


@pytest.mark.asyncio
async def test_show_variables(
    proxy_server_start,
):
    random_key = str(time.time_ns())
    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            await cursor.execute(
                key=random_key,
                query="SHOW VARIABLES",
            )
            res = await cursor.fetchall()
            assert isinstance(res, list)
            for item in res:
                assert isinstance(item, dict)


@pytest.mark.asyncio
async def test_load_infile(
    proxy_server_start,
):
    random_key = str(time.time_ns())
    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            resource_dirs = os.path.join(os.path.dirname(__file__), "resources")
            resource_user = os.path.join(resource_dirs, "user.csv")
            with pytest.raises(MmyLocalInfileUnsupportError):
                await cursor.execute(
                    key=random_key,
                    query='LOAD DATA LOCAL INFILE "%s" INTO TABLE user FIELDS TERMINATED BY "," (@1) SET name=@1'
                    % (resource_user),
                )


@pytest.mark.asyncio
async def test_diffrent_mysql_server_error(
    proxy_server_start,
    mocker,
):
    def _select_mysql_host(_cont: container) -> _Server:
        return _Server(
            host=_cont.host,
            port=_cont.port,
        )

    async def _select_mysql_host1(*args, **kwargs) -> _Server:
        return _select_mysql_host(DockerMySQL.MySQL1.value)

    async def _select_mysql_host2(*args, **kwargs) -> _Server:
        return _select_mysql_host(DockerMySQL.MySQL2.value)

    mocker.patch("mmy.mysql.proxy.select_mysql_host", side_effect=_select_mysql_host1)
    random_key = str(time.time_ns())
    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        mocker.patch(
            "mmy.mysql.proxy.select_mysql_host", side_effect=_select_mysql_host2
        )
        cursor = await connect.cursor()
        async with cursor:
            with pytest.raises(MmyUnmatchServerError):
                await cursor.execute(
                    key=random_key,
                    query="SHOW VARIABLES LIKE 'time_zone'",
                )
                res = await cursor.fetchall()
                assert isinstance(res, list)
                for item in res:
                    assert isinstance(item, dict)


@pytest.mark.asyncio
async def test_broken_host(
    proxy_server_start,
    mocker,
):
    mocker.patch("mmy.mysql.proxy.select_mysql_host", side_effect=MySQLHostBroken)
    random_key = str(time.time_ns())
    with pytest.raises(MySQLHostBroken):
        async with fix_proxy_connect(random_key, proxy_server_start) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.ping()


@pytest.mark.asyncio
async def test_broken_host_on_command_phase(
    proxy_server_start,
    mocker,
):
    random_key = str(time.time_ns())
    async with fix_proxy_connect(random_key, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            with pytest.raises(MySQLHostBroken):
                mocker.patch(
                    "mmy.mysql.proxy.select_mysql_host",
                    side_effect=MySQLHostBroken,
                )
                await cursor.execute(
                    key=random_key,
                    query="SHOW VARIABLES LIKE 'time_zone'",
                )
