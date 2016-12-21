package cattleevents

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/util"
	"strings"
	"time"
)

type backupHandlers struct {
}

func (h *backupHandlers) Create(event *revents.Event, cli *client.RancherClient) error {
	logrus.Infof("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)
	backup, err := h.decodeEventBackup(event)
	if err != nil {
		return err
	}

	volClient := newVolumeClient(&backup.Snapshot)

	logrus.Infof("Creating backup %v", backup.UUID)

	target := newBackupTarget(backup)
	status, err := volClient.createBackup(backup.Snapshot.UUID, backup.UUID, target)
	if err != nil {
		return err
	}

	err = util.Backoff(time.Hour*12, fmt.Sprintf("Failed waiting for restore to backup :%s", "somedir"), func() (bool, error) {
		s, err := volClient.reloadStatus(status)
		if err != nil {
			return false, err
		}
		if s.State == "done" {
			return true, nil
		} else if s.State == "error" {
			return false, fmt.Errorf("Backup create failed. Status: %v", s.Message)
		}
		return false, nil
	})

	if err != nil {
		return err
	}

	status, err = volClient.reloadStatus(status)
	if err != nil {
		return err
	}

	uri := strings.TrimSpace(status.Message)
	backupUpdates := map[string]interface{}{"uri": uri}
	eventDataWrapper := map[string]interface{}{"backup": backupUpdates}

	reply := newReply(event)
	reply.ResourceType = "backup"
	reply.ResourceId = event.ResourceID
	reply.Data = eventDataWrapper

	logrus.Infof("Reply: %+v", reply)
	return publishReply(reply, cli)

}

func (h *backupHandlers) Delete(event *revents.Event, cli *client.RancherClient) error {
	logrus.Infof("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)

	backup, err := h.decodeEventBackup(event)
	if err != nil {
		return err
	}

	volClient := newVolumeClient(&backup.Snapshot)

	logrus.Infof("Removing backup %v", backup.UUID)
	target := newBackupTarget(backup)
	if _, err := volClient.removeBackup(backup.Snapshot.UUID, backup.UUID, backup.URI, target); err != nil {
		return err
	}

	return reply("backup", event, cli)
}

func (h *backupHandlers) decodeEventBackup(event *revents.Event) (*eventBackup, error) {
	backup := &eventBackup{}
	if s, ok := event.Data["backup"]; ok {
		err := mapstructure.Decode(s, backup)
		return backup, err
	}
	return nil, fmt.Errorf("Event doesn't contain backup data. Event: %#v", event)
}

func newBackupTarget(backup *eventBackup) backupTarget {
	return backupTarget{
		Name:      backup.BackupTarget.Name,
		UUID:      backup.BackupTarget.UUID,
		NFSConfig: backup.BackupTarget.Data.Fields.NFSConfig,
	}
}
