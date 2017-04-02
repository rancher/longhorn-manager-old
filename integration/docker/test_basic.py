import socket

import pytest
import cattle

ORC = 'http://localhost:7000'

SIZE = str(16 * 1024 * 1024)
VOLUME_NAME = "test_vol-1.0"


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


def test_volume(client):
    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    volumes = client.list_volume()
    assert len(volumes) == 1
    assert volumes[0]["name"] == VOLUME_NAME
    assert volumes[0]["size"] == SIZE
    assert volumes[0]["numberOfReplicas"] == 2
    assert volumes[0]["state"] == "detached"

    volumeByName = client.by_id_volume(VOLUME_NAME)
    assert volumeByName["name"] == VOLUME_NAME
    assert volumeByName["size"] == SIZE
    assert volumeByName["numberOfReplicas"] == 2
    assert volumeByName["state"] == "detached"

    hosts = client.list_host()
    assert len(hosts) == 1

    host = hosts[0]
    assert host["uuid"] is not None
    assert host["name"] == socket.gethostname()
    assert host["address"] is not None

    volume.attach(hostId=host["uuid"])

    # FIXME should able to use volume = client.update(volume)
    volume = client.by_id_volume(VOLUME_NAME)
    volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0
