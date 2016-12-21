package cattleevents

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"

	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/v2"
)

const (
	deleteURL = "http://driver/v1/volumes/%s"
)

func ConnectToEventStream(conf Config) error {
	logrus.Infof("Listening for cattle events")

	nh := noopHandler{}
	ph := PingHandler{}
	volume := &volumeHandlers{}
	snapshot := &snapshotHandlers{}
	backup := &backupHandlers{}

	eventHandlers := map[string]revents.EventHandler{
		"storage.snapshot.create":          snapshot.Create,
		"storage.snapshot.remove":          snapshot.Delete,
		"storage.backup.create":            backup.Create,
		"storage.backup.remove":            backup.Delete,
		"storage.volume.remove":            volume.VolumeRemove,
		"storage.volume.reverttosnapshot":  volume.RevertToSnapshot,
		"storage.volume.restorefrombackup": volume.RestoreFromBackup,
		"storage.volume.activate":          nh.Handler,
		"storage.volume.deactivate":        nh.Handler,
		"ping": ph.Handler,
	}

	router, err := revents.NewEventRouter("", 0, conf.CattleURL, conf.CattleAccessKey, conf.CattleSecretKey, nil, eventHandlers, "", conf.WorkerCount, revents.DefaultPingConfig)
	if err != nil {
		return err
	}
	err = router.StartWithoutCreate(nil)
	return err
}

type noopHandler struct{}

func (h *noopHandler) Handler(event *revents.Event, cli *client.RancherClient) error {
	logrus.Infof("Received and ignoring event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)
	return reply("volume", event, cli)
}

type PingHandler struct {
}

func (h *PingHandler) Handler(event *revents.Event, cli *client.RancherClient) error {
	return nil
}

func reply(resourceType string, event *revents.Event, cli *client.RancherClient) error {
	replyData := make(map[string]interface{})
	reply := newReply(event)
	reply.ResourceType = resourceType
	reply.ResourceId = event.ResourceID
	reply.Data = replyData
	logrus.Infof("Reply: %+v", reply)
	err := publishReply(reply, cli)
	if err != nil {
		return err
	}
	return nil
}

func newReply(event *revents.Event) *client.Publish {
	return &client.Publish{
		Name:        event.ReplyTo,
		PreviousIds: []string{event.ID},
	}
}

func publishReply(reply *client.Publish, apiClient *client.RancherClient) error {
	_, err := apiClient.Publish.Create(reply)
	return err
}

func decodeEvent(event *revents.Event, key string, target interface{}) error {
	if s, ok := event.Data[key]; ok {
		err := mapstructure.Decode(s, target)
		return err
	}
	return fmt.Errorf("Event doesn't contain %v data. Event: %#v", key, event)
}

type processData struct {
	ProcessID  string `mapstructure:"processId"`
	VolumeName string
}

type eventBackup struct {
	UUID         string
	URI          string
	Snapshot     eventSnapshot
	BackupTarget struct {
		Name string
		UUID string
		Data struct {
			Fields struct {
				NFSConfig nfsConfig
			}
		}
	}
}

type eventSnapshot struct {
	UUID   string
	Volume struct {
		Name string
		UUID string
	}
}

type Config struct {
	CattleURL       string
	CattleAccessKey string
	CattleSecretKey string
	WorkerCount     int
}
