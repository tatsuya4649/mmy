import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from python_on_whales import docker
from rich import print
from src.mysql.hosts import MySQLHosts
from src.mysql.proxy import proxy_handler
from src.mysql.proxy_connection import ProxyConnection, proxy_connect

from .test_mysql import DockerMySQL, container

HOST = "127.0.0.1"
_MYSQL_HOSTS = MySQLHosts()
PROXY_SERVER_STARTUP_TIMEOUT: int = 10


@pytest.fixture()
def proxy_server_start(event_loop: asyncio.AbstractEventLoop, unused_tcp_port: int):
    async def _run_proxy_server():
        _server = asyncio.start_server(
            lambda r, w: proxy_handler(
                mysql_hosts=_MYSQL_HOSTS,
                client_reader=r,
                client_writer=w,
                connection_timeout=1,
                command_timeout=1,
            ),
            HOST,
            unused_tcp_port,
        )
        _s = await _server
        async with _s:
            await _s.serve_forever()

    proxy_server = event_loop.create_task(_run_proxy_server())
    # Waiting for start up server
    async def _ping_proxy_server():
        while True:
            try:
                _, writer = await asyncio.open_connection(HOST, unused_tcp_port)
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

    event_loop.run_until_complete(_timeout_ping())
    yield unused_tcp_port

    async def _cancel_server():
        proxy_server.cancel()
        with pytest.raises(asyncio.CancelledError):
            await proxy_server
        await asyncio.sleep(0.1)

    event_loop.run_until_complete(_cancel_server())
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


@pytest.mark.asyncio
async def test_proxy(
    proxy_server_start,
):
    random_key = str(time.time_ns())

    _client: ProxyConnection = proxy_connect(
        key=random_key,
        host=HOST,
        port=proxy_server_start,
        db="test",
        user="root",
        password="root",
    )
    async with _client as connect:
        await connect.ping()


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
    _client: ProxyConnection = proxy_connect(
        key=random_key,
        host=HOST,
        port=proxy_server_start,
        db="test",
        user="root",
        password="root",
        ssl=ctx,
    )
    async with _client as connect:
        await connect.ping()
