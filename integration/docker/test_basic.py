import os
import string

import cattle

ENV_ORC_IPS = "LONGHORN_ORC_TEST_SERVER_IPS"

ORC = 'http://localhost:7000'

SIZE = str(16 * 1024 * 1024)
VOLUME_NAME = "longhorn-orc-test_vol-1.0"


def get_client(ip):
    url = 'http://' + ip + ':7000/v1/schemas'
    c = cattle.from_env(url=url)
    return c


def get_orc_ips():
    return string.split(os.environ[ENV_ORC_IPS], ",")


def test_host_list():
    ips = get_orc_ips()
    client = get_client(ips[0])

    hosts = client.list_host()
    assert len(hosts) == 1

    host = hosts[0]
    assert host["uuid"] is not None
    assert host["address"] is not None

    new_host = client.by_id_host(host["uuid"])
    assert new_host["uuid"] == host["uuid"]
    assert new_host["name"] == host["name"]
    assert new_host["address"] == host["address"]


def test_volume():
    ips = get_orc_ips()
    client = get_client(ips[0])

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
    assert host["address"] is not None

    volume = volume.attach(hostId=host["uuid"])

    # FIXME should able to use volume = client.update(volume)
    volume = client.by_id_volume(VOLUME_NAME)

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0
