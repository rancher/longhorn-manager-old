import socket

import pytest
import cattle

ORC = 'http://localhost:7000'


@pytest.fixture
def client(request):
    url = 'http://localhost:7000/v1/schemas'
    c = cattle.from_env(url=url)
    return c


def test_host_list(client):
    hosts = client.list_host()
    assert len(hosts) == 1

    host = hosts[0]
    assert host["uuid"] is not None
    assert host["name"] == socket.gethostname()
    assert host["address"] is not None

    new_host = client.by_id_host(host["uuid"])
    assert new_host["uuid"] == host["uuid"]
    assert new_host["name"] == host["name"]
    assert new_host["address"] == host["address"]
