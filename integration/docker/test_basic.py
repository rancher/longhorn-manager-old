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


def test_snapshot():
    ips = get_orc_ips()
    client = get_client(ips[0])

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    hosts = client.list_host()
    assert len(hosts) == 1

    host = hosts[0]
    assert host["uuid"] is not None
    assert host["address"] is not None

    volume = volume.attach(hostId=host["uuid"])

    snap1 = volume.snapshotCreate()
    snap2 = volume.snapshotCreate()
    snap3 = volume.snapshotCreate()

    snapshots = volume.snapshotList()
    assert len(snapshots) == 3

    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert len(snapMap) == 3
    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["removed"] is False

    volume.snapshotDelete(name=snap3["name"])

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    assert len(snapshots) == 3
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert len(snapMap) == 3
    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == ["volume-head"]
    assert snapMap[snap3["name"]]["removed"] is True

    snap = volume.snapshotGet(name=snap3["name"])
    assert snap["name"] == snap3["name"]
    assert snap["parent"] == snap3["parent"]
    assert snap["children"] == snap3["children"]
    assert snap["removed"] is True

    volume.snapshotRevert(name=snap2["name"])

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    assert len(snapshots) == 3
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert len(snapMap) == 3
    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert snapMap[snap2["name"]]["children"] == ["volume-head", snap3["name"]]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == []
    assert snapMap[snap3["name"]]["removed"] is True

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0
