import asyncio
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pformat
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from mmy.etcd import MySQLEtcdClient
from mmy.mysql.hosts import MySQLHostBroken, MySQLHosts
from mmy.mysql.proxy import ProxyServer
from mmy.mysql.proxy_connection import ProxyConnection, proxy_connect
from mmy.mysql.proxy_err import MmyLocalInfileUnsupportError, MmyUnmatchServerError
from mmy.server import Server, State, _Server
from python_on_whales import docker
from rich import print

from ._mysql import TEST_TABLE2, mmy_info
from .test_mysql import DockerMySQL, container, up_mysql_docker_container

HOST = "127.0.0.1"
PROXY_SERVER_STARTUP_TIMEOUT: int = 10

logger = logging.getLogger(__name__)


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


def fix_proxy_connect(key: str, port: int, timeout: float | int = 0.1):
    _client: ProxyConnection = proxy_connect(
        key=key,
        host=HOST,
        port=port,
        db="test",
        user="root",
        password="root",
        connect_timeout=timeout,
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
                query="SELECT * FROM %s LIMIT %d" % (TEST_TABLE2, length),
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
                query="INSERT INTO %s (name) VALUES (%s)" % (TEST_TABLE2, random_key),
            )
            assert res == 1

        await connect.commit()


count = 10000


@pytest.mark.asyncio
async def test_insert_many(
    proxy_server_start,
):
    import random

    random_key = random.randint(1000000, 10000000)
    keys = list()
    for i in range(count):
        if i % 100 == 0:
            logger.info(f"Complete: {int(100*(i/count))}%")

        _rk = str(random_key + i)
        keys.append(_rk)
        async with fix_proxy_connect(_rk, proxy_server_start) as connect:
            cursor = await connect.cursor()
            async with cursor:
                res = await cursor.execute(
                    key=_rk,
                    query="INSERT INTO %s (id, name) VALUES (%s, %s)"
                    % (TEST_TABLE2, _rk, _rk),
                )
                assert res == 1

            await connect.commit()

        async with fix_proxy_connect(_rk, proxy_server_start) as connect:
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    key=_rk,
                    query="SELECT * FROM %s WHERE id=%s" % (TEST_TABLE2, _rk),
                )
                res = len(await cursor.fetchall())
                assert res == 1


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
                % (TEST_TABLE2, random_id, str(random_key)),
            )
            assert res == 1

            new_name = str(time.time_ns())
            res = await cursor.execute(
                key=str(random_key),
                query="UPDATE %s SET name=%s WHERE id=%d"
                % (TEST_TABLE2, new_name, random_id),
            )
            assert res == 1

        await connect.commit()


@pytest.mark.asyncio
async def test_update_many(
    proxy_server_start,
):
    import random

    random_key = random.randint(1000000, 10000000)
    keys = list()
    for i in range(count):
        if i % 100 == 0:
            logger.info(f"Complete: {int(100*(i/count))}%")

        _rk = str(random_key + i)
        keys.append(_rk)
        async with fix_proxy_connect(_rk, proxy_server_start) as connect:
            cursor = await connect.cursor()
            async with cursor:
                res = await cursor.execute(
                    key=_rk,
                    query="INSERT INTO %s (id, name) VALUES (%s, %s)"
                    % (TEST_TABLE2, _rk, _rk),
                )
                assert res == 1

            await connect.commit()

        async with fix_proxy_connect(_rk, proxy_server_start) as connect:
            _random_key = random.randint(1000000, 10000000)
            _nrk = str(_random_key)
            cursor = await connect.cursor()
            async with cursor:
                await cursor.execute(
                    key=_rk,
                    query="UPDATE %s SET name=%s" % (TEST_TABLE2, _nrk),
                )
                res = len(await cursor.fetchall())
                assert res == 1


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
                query="INSERT INTO %s (name) VALUES (%s)" % (TEST_TABLE2, random_key),
            )
            assert res == 1

        await connect.commit()

        cursor = await connect.cursor()
        async with cursor:
            res = await cursor.execute(
                key=random_key,
                query="DELETE FROM %s WHERE name=%s" % (TEST_TABLE2, random_key),
            )
            assert res == 1

        await connect.commit()


@pytest.mark.asyncio
async def test_delete_many(
    proxy_server_start,
):
    import random

    random_key = random.randint(1000000, 10000000)
    selects = list()
    _rk = str(random_key)
    async with fix_proxy_connect(_rk, proxy_server_start) as connect:
        cursor = await connect.cursor()
        async with cursor:
            await cursor.execute(
                key=_rk,
                query="SELECT * FROM %s LIMIT %d" % (TEST_TABLE2, count),
            )
            selects.extend(await cursor.fetchall())

    for index, row in enumerate(selects):
        if index % 100 == 0:
            logger.info(f"Complete: {int(100*(index/count))}%")

        async with fix_proxy_connect(_rk, proxy_server_start) as connect:
            cursor = await connect.cursor()
            async with cursor:
                res = await cursor.execute(
                    key=_rk,
                    query="DELETE FROM %s WHERE id=%s" % (TEST_TABLE2, row["id"]),
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
            resource_user = os.path.join(resource_dirs, "post.csv")
            with pytest.raises(MmyLocalInfileUnsupportError):
                await cursor.execute(
                    key=random_key,
                    query='LOAD DATA LOCAL INFILE "%s" INTO TABLE post FIELDS TERMINATED BY "," (@1) SET name=@1'
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


@pytest.mark.asyncio
async def test_update_etcd_data(
    up_etcd_docker_containers,
    unused_tcp_port: int,
    mmy_info,
):
    import random

    from mmy.cmd.proxy import etcd_management
    from mmy.etcd import MySQLEtcdData
    from pymysql.err import OperationalError

    from .test_etcd import DockerEtcd
    from .test_mysql import get_mysql_docker_for_test, up_mysql_docker_container

    mysql_hosts = MySQLHosts()

    COUNT = 1000
    RETRY_COUNT = 4
    RETRY_INTERVAL = 2

    async def _do_update_etcd_data():
        etcds = list(DockerEtcd)
        mysqls = list(DockerMySQL)
        for node in etcds:
            try:
                cli = MySQLEtcdClient(
                    host=node.value.host,
                    port=node.value.port,
                )
                randint = random.randint(1, len(mysqls))
                random_mysqls = random.sample(mysqls, randint)
                logger.info("Put new MySQL nodes into etcd")
                await cli.put(
                    data=MySQLEtcdData(
                        nodes=[
                            Server(
                                host=i.value.host,
                                port=i.value.port,
                                state=State.Run,
                            )
                            for i in random_mysqls
                        ]
                    )
                )
                break
            except Exception as e:
                logger.exception(e)
                continue
        else:
            raise RuntimeError

    async def _check_mysql():
        """

        Waiting for etcd to have more than 1 MySQL nodes.

        """
        logger.debug("Test MySQL servers on etcd")
        _retry_count: int = 10
        for _ in range(_retry_count):
            try:
                etcds = list(DockerEtcd)
                for node in etcds:
                    try:
                        cli = MySQLEtcdClient(
                            host=node.value.host,
                            port=node.value.port,
                        )
                        data: MySQLEtcdData = await cli.get()
                        if len(data.nodes) == 0:
                            raise RuntimeError("No MySQL nodes")
                        else:
                            mysql_hosts.update(data.nodes)
                            logger.info(
                                f"MySQL servers. {[i.address_format() for i in data.nodes]}"
                            )
                        return
                    except Exception as e:
                        await asyncio.sleep(1)
                        logger.error(e)
                        continue
                else:
                    raise RuntimeError
            except Exception as e:
                logger.warning(e)
                await asyncio.sleep(1)
                continue
        else:
            raise RuntimeError

    # Initial etcd node
    await _do_update_etcd_data()
    await _check_mysql()

    async with run_proxy_server(
        mysql_hosts=mysql_hosts,
        unused_tcp_port=unused_tcp_port,
    ):

        async def _main():
            for i in range(COUNT):
                if i % 100 == 0:
                    logger.info(f"Complete {int(100*i/COUNT)}%")
                random_key = str(time.time_ns())
                sleep_time = random.uniform(0, 0.3)
                await asyncio.sleep(sleep_time)
                last_e: Exception | None = None
                for j in range(RETRY_COUNT):
                    if j > 0:
                        logger.warning(j)

                    try:
                        async with fix_proxy_connect(
                            random_key,
                            unused_tcp_port,
                            timeout=10.0,
                        ) as connect:
                            cursor = await connect.cursor()
                            async with cursor:
                                await cursor.execute(
                                    key=random_key,
                                    query="SHOW VARIABLES LIKE 'time_zone'",
                                )
                        break
                    except OperationalError as e:
                        # Proxy server maybe graceful restart now
                        logger.exception(e)
                        last_e = e
                        errcode = e.args[0]
                        if errcode == 2003:
                            await asyncio.sleep(RETRY_INTERVAL)
                            continue

                else:
                    raise last_e

        async def _update_etcd_data():
            while True:
                sleep_time = random.uniform(0, 0.5)
                await asyncio.sleep(sleep_time)
                await _do_update_etcd_data()

        async def update_mysql_hosts(
            mysql_hosts=mysql_hosts,
        ):
            while True:
                await asyncio.sleep(1)
                logger.info(f"Monitor hosts: {pformat(mysql_hosts.get_hosts())}")

        _t1 = asyncio.create_task(_main())
        _t2 = asyncio.create_task(_update_etcd_data())
        _t3 = asyncio.create_task(
            etcd_management(
                mysql_hosts=mysql_hosts,
                etcd_data=mmy_info.etcd,
            )
        )
        _t4 = asyncio.create_task(
            update_mysql_hosts(
                mysql_hosts=mysql_hosts,
            )
        )
        _, pending = await asyncio.wait(
            [_t1, _t2, _t3, _t4],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for i in pending:
            i.cancel()
            with pytest.raises(asyncio.CancelledError):
                await i
