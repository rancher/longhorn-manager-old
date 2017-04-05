import os
import string
import time

import cattle

ENV_ORC_IPS = "LONGHORN_ORC_TEST_SERVER_IPS"
ENV_BACKUPSTORE_URL = "LONGHORN_ORC_TEST_BACKUPSTORE_URL"

ORC = 'http://localhost:9500'

SIZE = str(16 * 1024 * 1024)
VOLUME_NAME = "longhorn-orc-test_vol-1.0"


def get_client(ip):
    url = 'http://' + ip + ':9500/v1/schemas'
    c = cattle.from_env(url=url)
    return c


def get_orc_ips():
    return string.split(os.environ[ENV_ORC_IPS], ",")


def get_backupstore_url():
    return os.environ[ENV_BACKUPSTORE_URL]


def test_host_and_settings():
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
    assert "volume-head" in snapMap[snap2["name"]]["children"]
    assert snap3["name"] in snapMap[snap2["name"]]["children"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == []
    assert snapMap[snap3["name"]]["removed"] is True

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0


def test_backup():
    ips = get_orc_ips()
    client = get_client(ips[0])

    setting = client.by_id_setting("backupTarget")
    setting = client.update(setting, value=get_backupstore_url())
    assert setting["value"] == get_backupstore_url()

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

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0
