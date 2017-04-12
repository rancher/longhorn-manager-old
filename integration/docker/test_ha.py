import time

from common import clients  # NOQA
from common import SIZE, VOLUME_NAME, DEV_PATH

def test_ha_simple_recovery(clients):  # NOQA
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

    volume = volume.attach(hostId=host_id)
    assert volume["state"] == "healthy"

    volume = client.by_id_volume(VOLUME_NAME)
    assert volume["endpoint"] == DEV_PATH + VOLUME_NAME

    assert len(volume["replicas"]) == 2
    replica0 = volume["replicas"][0]
    assert replica0["name"] != ""

    replica1 = volume["replicas"][1]
    assert replica1["name"] != ""

    volume = volume.replicaRemove(name=replica0["name"])
    assert volume["state"] == "degraded"
    assert len(volume["replicas"]) == 1

    time.sleep(5)

    volume = client.by_id_volume(VOLUME_NAME)
    assert volume["state"] == "healthy"
    assert len(volume["replicas"]) == 2

    new_replica0 = volume["replicas"][0]
    new_replica1 = volume["replicas"][1]

    assert (replica1["name"] == new_replica0["name"] or
            replica1["name"] == new_replica1["name"])

    volume = volume.detach()

    client.delete(volume)

    volumes = client.list_volume()
    assert len(volumes) == 0
