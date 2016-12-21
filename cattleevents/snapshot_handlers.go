package cattleevents

import (
	"github.com/Sirupsen/logrus"
	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/v2"
)

type snapshotHandlers struct {
}

func (h *snapshotHandlers) Create(event *revents.Event, cli *client.RancherClient) error {
	logrus.Infof("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)

	snapshot := &eventSnapshot{}
	err := decodeEvent(event, "snapshot", snapshot)
	if err != nil {
		return err
	}

	volClient := newVolumeClient(snapshot)

	found, _ := volClient.getSnapshot(snapshot.UUID)
	if found != nil {
		return reply("snapshot", event, cli)
	}

	logrus.Infof("Creating snapshot %v", snapshot.UUID)

	if _, err := volClient.createSnapshot(snapshot.UUID); err != nil {
		return err
	}

	return reply("snapshot", event, cli)
}

func (h *snapshotHandlers) Delete(event *revents.Event, cli *client.RancherClient) error {
	logrus.Infof("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)

	snapshot := &eventSnapshot{}
	err := decodeEvent(event, "snapshot", snapshot)
	if err != nil {
		return err
	}

	volClient := newVolumeClient(snapshot)
	if err := volClient.deleteSnapshot(snapshot.UUID); err != nil {
		return err
	}

	return reply("snapshot", event, cli)
}
