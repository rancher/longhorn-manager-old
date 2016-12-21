package cattle

import (
	"errors"

	log "github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/util"
)

type StoragePoolManager interface {
	SyncStoragePool(string, []string) error
}

type mgr struct {
	rancherClient *client.RancherClient
}

func NewCattleClient(cattleURL, cattleAccessKey, cattleSecretKey string) (StoragePoolManager, error) {
	if cattleURL == "" {
		return nil, errors.New("cattle url is empty")
	}

	apiClient, err := client.NewRancherClient(&client.ClientOpts{
		Url:       cattleURL,
		AccessKey: cattleAccessKey,
		SecretKey: cattleSecretKey,
	})

	if err != nil {
		return nil, err
	}

	return &mgr{
		rancherClient: apiClient,
	}, nil
}

func (c *mgr) SyncStoragePool(driver string, hostUuids []string) error {
	log.Debugf("storagepool event %v", hostUuids)
	sp := client.StoragePool{
		Name:               driver,
		ExternalId:         driver,
		DriverName:         driver,
		VolumeAccessMode:   "singleHostRW",
		BlockDevicePath:    util.DevDir,
		VolumeCapabilities: []string{"snapshot"},
	}
	espe := &client.ExternalStoragePoolEvent{
		EventType:   "storagepool.create",
		HostUuids:   hostUuids,
		ExternalId:  driver,
		StoragePool: sp,
	}
	_, err := c.rancherClient.ExternalStoragePoolEvent.Create(espe)
	return err
}
