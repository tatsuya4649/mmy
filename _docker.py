from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from python_on_whales import docker
from rich import print


def gen_private_key():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    print(private_key)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    print(pem)
    return pem


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


from typing import Any


def copy_ca_from_docker() -> tuple[x509.Certificate, Any]:
    import tempfile

    TMP = "/tmp"
    MYSQL_DRI = "/var/lib/mysql"
    _CA_FILENAME = "/ca.pem"
    _CA_KEY_FILENAME = "/ca-key.pem"
    CA_PATH = MYSQL_DRI + _CA_FILENAME
    CA_KEY_PATH = MYSQL_DRI + _CA_KEY_FILENAME

    with tempfile.TemporaryDirectory() as dirname:
        docker.copy(("mmy1", CA_PATH), dirname)
        with open(dirname + _CA_FILENAME, "rb") as f:
            _pem_data = f.read()
            ca: x509.Certificate = x509.load_pem_x509_certificate(_pem_data)
            print(ca)

        docker.copy(("mmy1", CA_KEY_PATH), dirname)
        with open(dirname + _CA_KEY_FILENAME, "rb") as f:
            _pem_data = f.read()
            ca_key = serialization.load_pem_private_key(
                data=_pem_data,
                password=None,
            )
            print(ca_key)

        return (ca, ca_key)


copy_ca_from_docker()
