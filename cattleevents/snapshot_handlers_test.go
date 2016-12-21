package cattleevents

import (
	"encoding/json"
	"testing"

	revents "github.com/rancher/event-subscriber/events"
)

var snapshotEvent = `{"name":"asdf",
	"data":{"snapshot":{"name":"","id":3,"state":"creating","description":"","created":1464108273000,"data":{},"accountId":5,
	"volumeId":126,"kind":"snapshot","removed":"","uuid":"snap-uuid", "type":"snapshot",
	"volume":{"name":"expected-vol-name","id":126,"state":"active","created":1464107700000,
	"data":{"fields":{"driver":"longhorn","capabilities":["snapshot"]}},"accountId":5,"kind":"volume","zoneId":1,"hostId":1,
	"uuid":"expected-vol-uuid","externalId":"vd1","accessMode":"singleHostRW",
	"allocationState":"active","type":"volume"}}
	}}`

var backupEvent = `
{
  "id": "ed59097c-e470-48fc-b45c-32ec7fed3c07",
  "name": "storage.backup.create",
  "replyTo": "reply.2806044353996486871",
  "resourceId": "1b1",
  "resourceType": "backup",
  "data": {
    "backup": {
      "id": 1,
      "state": "creating",
      "created": 1465276634000,
      "uuid": "424c996d-2050-4ea2-85cf-0351989e91ec",
      "accountId": 5,
      "snapshotId": 1,
      "kind": "backup",
      "volumeId": 32,
      "backupTargetId": 1,
      "type": "backup",
      "snapshot": {
        "id": 1,
        "uuid": "f690052a-956d-41f4-ba61-d7a1a88de652",
        "kind": "snapshot",
        "type": "snapshot"
      },
      "backupTarget": {
        "name": "name",
        "id": 1,
        "state": "created",
        "created": 1465276622000,
        "removed": null,
        "removeTime": null,
        "description": "",
        "uuid": "auuid",
        "accountId": 5,
        "kind": "backupTarget",
        "data": {
          "fields": {
            "nfsConfig": {
              "server": "1.2.3.5",
              "share": "/var/nfs"
            }
          }
        },
        "type": "backupTarget"
      }
    }
  }
}
`

func TestSnapshotEvent(t *testing.T) {
	event := createEvent(snapshotEvent, t)

	snapshotData := &eventSnapshot{}
	err := decodeEvent(event, "snapshot", snapshotData)
	if err != nil {
		t.Fatal(err)
	}

	if snapshotData.Volume.Name != "expected-vol-name" || snapshotData.Volume.UUID != "expected-vol-uuid" {
		t.Fatalf("Unexpected: %v %v", snapshotData.Volume.Name, snapshotData.Volume.UUID)
	}
}

func TestBackupEvent(t *testing.T) {
	event := createEvent(backupEvent, t)

	backupData := &eventBackup{}
	err := decodeEvent(event, "backup", backupData)
	if err != nil {
		t.Fatal(err)
	}

	conf := backupData.BackupTarget.Data.Fields.NFSConfig
	if backupData.BackupTarget.Name != "name" || backupData.BackupTarget.UUID != "auuid" ||
		conf.Server != "1.2.3.5" || conf.Share != "/var/nfs" || conf.MountOptions != "" {
		t.Fatalf("Unexpected: %v", backupData.BackupTarget)
	}
}

func createEvent(eventData string, t *testing.T) *revents.Event {
	eventJSON := []byte(eventData)
	event := &revents.Event{}
	err := json.Unmarshal(eventJSON, event)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return event
}
