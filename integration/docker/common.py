import os
import string

import pytest

import cattle

ENV_ORC_IPS = "LONGHORN_ORC_TEST_SERVER_IPS"
ENV_BACKUPSTORE_URL = "LONGHORN_ORC_TEST_BACKUPSTORE_URL"

ORC = 'http://localhost:9500'

SIZE = str(16 * 1024 * 1024)
VOLUME_NAME = "longhorn-orc-test_vol-1.0"
DEV_PATH = "/dev/longhorn/"

PORT = ":9500"


@pytest.fixture
def clients(request):
    ips = get_orc_ips()
    client = get_client(ips[0] + PORT)
    hosts = client.list_host()
    assert len(hosts) == len(ips)
    clis = get_clients(hosts)
    request.addfinalizer(lambda: cleanup_clients(clis))
    cleanup_clients(clis)
    return clis


def cleanup_clients(clis):
    client = clis.itervalues().next()
    volumes = client.list_volume()
    for v in volumes:
        client.delete(v)


def get_client(address):
    url = 'http://' + address + '/v1/schemas'
    c = cattle.from_env(url=url)
    return c


def get_orc_ips():
    return string.split(os.environ[ENV_ORC_IPS], ",")


def get_backupstore_url():
    return os.environ[ENV_BACKUPSTORE_URL]


def get_clients(hosts):
    clients = {}
    for host in hosts:
        assert host["uuid"] is not None
        assert host["address"] is not None
        clients[host["uuid"]] = get_client(host["address"])
    return clients
