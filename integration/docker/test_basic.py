import os
import string
import time

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


def test_hosts_and_settings(clients):
    hosts = clients.itervalues().next().list_host()
    assert hosts[0]["uuid"] is not None
    assert hosts[0]["address"] is not None
    assert hosts[1]["uuid"] is not None
    assert hosts[1]["address"] is not None
    assert hosts[2]["uuid"] is not None
    assert hosts[2]["address"] is not None

    host0_id = hosts[0]["uuid"]
    host1_id = hosts[1]["uuid"]
    host2_id = hosts[2]["uuid"]
    host0_from0 = clients[host0_id].by_id_host(host0_id)
    host0_from1 = clients[host1_id].by_id_host(host0_id)
    host0_from2 = clients[host2_id].by_id_host(host0_id)
    assert host0_from0["uuid"] == \
        host0_from1["uuid"] == \
        host0_from2["uuid"]
    assert host0_from0["address"] == \
        host0_from1["address"] == \
        host0_from2["address"]

    client = clients[host0_id]

    settings = client.list_setting()
    assert len(settings) == 2

    settingMap = {}
    for setting in settings:
        settingMap[setting["name"]] = setting

    assert settingMap["backupTarget"]["value"] != ""
    assert settingMap["longhornImage"]["value"] != ""

    setting = client.by_id_setting("longhornImage")
    assert settingMap["longhornImage"]["value"] == setting["value"]

    setting = client.by_id_setting("backupTarget")
    assert settingMap["backupTarget"]["value"] == setting["value"]

    old_target = setting["value"]

    setting = client.update(setting, value="testbackup")
    assert setting["value"] == "testbackup"
    setting = client.by_id_setting("backupTarget")
    assert setting["value"] == "testbackup"

    setting = client.update(setting, value=old_target)
    assert setting["value"] == old_target


def test_volume(clients):
    # get a random client
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"
    assert volume["created"] != ""

    volumes = client.list_volume()
    assert len(volumes) == 1
    assert volumes[0]["name"] == volume["name"]
    assert volumes[0]["size"] == volume["size"]
    assert volumes[0]["numberOfReplicas"] == volume["numberOfReplicas"]
    assert volumes[0]["state"] == volume["state"]
    assert volumes[0]["created"] == volume["created"]

    volumeByName = client.by_id_volume(VOLUME_NAME)
    assert volumeByName["name"] == volume["name"]
    assert volumeByName["size"] == volume["size"]
    assert volumeByName["numberOfReplicas"] == volume["numberOfReplicas"]
    assert volumeByName["state"] == volume["state"]
    assert volumeByName["created"] == volume["created"]

    volume = volume.attach(hostId=host_id)

    volume = client.by_id_volume(VOLUME_NAME)
    assert volume["endpoint"] == DEV_PATH + VOLUME_NAME

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0


def test_recurring_snapshot(clients):
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)

    snap3s = {"name": "snap3s", "cron": "@every 3s", "task": "snapshot"}
    snap5s = {"name": "snap5s", "cron": "@every 5s", "task": "snapshot"}
    volume.scheduleUpdate(jobs=[snap3s, snap5s])

    volume = volume.attach(hostId=host_id)

    time.sleep(10)

    assert len(volume.snapshotList()) == 5


def test_recurring_snapshot_live_update(clients):
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)

    volume = volume.attach(hostId=host_id)

    snap3s = {"name": "snap3s", "cron": "@every 3s", "task": "snapshot"}
    snap5s = {"name": "snap5s", "cron": "@every 5s", "task": "snapshot"}
    volume.scheduleUpdate(jobs=[snap3s, snap5s])

    time.sleep(10)

    assert len(volume.snapshotList()) == 5


def test_snapshot(clients):
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    volume = volume.attach(hostId=host_id)
    snapshot_test(client)
    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0


def snapshot_test(client):
    volume = client.by_id_volume(VOLUME_NAME)

    snap1 = volume.snapshotCreate()
    snap2 = volume.snapshotCreate()
    snap3 = volume.snapshotCreate()

    snapshots = volume.snapshotList()
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

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
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

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
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert "volume-head" in snapMap[snap2["name"]]["children"]
    assert snap3["name"] in snapMap[snap2["name"]]["children"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == []
    assert snapMap[snap3["name"]]["removed"] is True

    volume.snapshotDelete(name=snap1["name"])
    volume.snapshotDelete(name=snap2["name"])

    volume.snapshotPurge()

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap
    assert snap1["name"] not in snapMap
    assert snap3["name"] not in snapMap

    # it's the parent of volume-head, so it cannot be purged at this time
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == ""
    assert "volume-head" in snapMap[snap2["name"]]["children"]
    assert snapMap[snap2["name"]]["removed"] is True


def test_backup(clients):
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    volume = volume.attach(hostId=host_id)
    backup_test(client)
    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0


def backup_test(client):
    volume = client.by_id_volume(VOLUME_NAME)

    setting = client.by_id_setting("backupTarget")
    setting = client.update(setting, value=get_backupstore_url())
    assert setting["value"] == get_backupstore_url()

    volume.snapshotCreate()
    snap2 = volume.snapshotCreate()
    volume.snapshotCreate()

    volume.snapshotBackup(name=snap2["name"])

    found = False
    for i in range(100):
        bvs = client.list_backupVolume()
        for bv in bvs:
            if bv["name"] == VOLUME_NAME:
                found = True
                break
        if found:
            break
        time.sleep(1)
    assert found

    found = False
    for i in range(20):
        backups = bv.backupList()
        for b in backups:
            if b["snapshotName"] == snap2["name"]:
                found = True
                break
        if found:
            break
        time.sleep(1)
    assert found

    new_b = bv.backupGet(name=b["name"])
    assert new_b["name"] == b["name"]
    assert new_b["url"] == b["url"]
    assert new_b["snapshotName"] == b["snapshotName"]
    assert new_b["snapshotCreated"] == b["snapshotCreated"]
    assert new_b["created"] == b["created"]
    assert new_b["volumeName"] == b["volumeName"]
    assert new_b["volumeSize"] == b["volumeSize"]
    assert new_b["volumeCreated"] == b["volumeCreated"]

    bv.backupDelete(name=b["name"])

    backups = bv.backupList()
    found = False
    for b in backups:
        if b["snapshotName"] == snap2["name"]:
            found = True
            break
    assert not found


def test_volume_multinode(clients):
    hosts = clients.keys()

    volume = clients[hosts[0]].create_volume(name=VOLUME_NAME, size=SIZE,
                                             numberOfReplicas=2)
    assert volume["state"] == "detached"

    for host_id in hosts:
        volume = volume.attach(hostId=host_id)
        assert volume["state"] == "healthy"
        assert volume["controller"]["hostId"] == host_id
        volume = volume.detach()

    volume = volume.attach(hostId=hosts[0])
    assert volume["state"] == "healthy"
    assert volume["controller"]["hostId"] == hosts[0]

    snapshot_test(clients[hosts[1]])
    backup_test(clients[hosts[2]])

    clients[hosts[1]].delete(volume)

    volumes = clients[hosts[2]].list_volume()
    assert len(volumes) == 0
