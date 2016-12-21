package driver

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	md "github.com/rancher/go-rancher-metadata/metadata"
	rancherClient "github.com/rancher/go-rancher/v2"

	"github.com/rancher/longhorn-orc/model"
	"github.com/rancher/longhorn-orc/util"
)

const (
	root                = "/var/lib/rancher/longhorn"
	RancherMetadataURL  = "http://rancher-metadata/2016-07-29"
	defaultVolumeSize   = "10g"
	optSize             = "size"
	optReadIOPS         = "read-iops"
	optWriteIOPS        = "write-iops"
	optReplicaBaseImage = "base-image"
	optDontFormat       = "dont-format"
)

func NewStorageDaemon(mdc util.MetadataConfig, client *rancherClient.RancherClient) (*StorageDaemon, error) {
	mdClient := md.NewClient(RancherMetadataURL)

	volumeStore := &volumeStore{
		mdClient: mdClient,
		rootDir:  root,
	}

	sd := &StorageDaemon{
		mdc:      mdc,
		rClient:  client,
		mdClient: mdClient,
		store:    volumeStore,
	}

	return sd, nil
}

type StorageDaemon struct {
	mdc      util.MetadataConfig
	rClient  *rancherClient.RancherClient
	mdClient md.Client
	store    *volumeStore
}

func volConfig(volume *model.Volume) (*volumeConfig, error) {
	sizeStr := volume.Opts[optSize]
	dontFormat, _ := strconv.ParseBool(volume.Opts[optDontFormat])
	var size string
	if sizeStr == "" {
		if dontFormat {
			sizeStr = "0b"
		} else {
			sizeStr = defaultVolumeSize
		}
		logrus.Infof("No size option provided. Using: %v", sizeStr)
	}
	size, sizeGB, err := util.ConvertSize(sizeStr)
	if err != nil {
		return nil, fmt.Errorf("Can't parse size %v. Error: %v", sizeStr, err)
	}

	volConfig := &volumeConfig{
		Name:             volume.Name,
		Size:             size,
		SizeGB:           sizeGB,
		ReadIOPS:         volume.Opts[optReadIOPS],
		WriteIOPS:        volume.Opts[optWriteIOPS],
		ReplicaBaseImage: volume.Opts[optReplicaBaseImage],
		DontFormat:       dontFormat,
	}
	return volConfig, nil
}

func (d *StorageDaemon) Create(volume *model.Volume) (*model.Volume, error) {
	logrus.Infof("Creating volume %v", volume)

	vc, err := volConfig(volume)
	if err != nil {
		return nil, err
	}
	stack := newStack(d, vc)

	if err := doCreateVolume(stack); err != nil {
		stack.Delete()
		return nil, fmt.Errorf("Error creating Rancher stack for volume %v: %v", volume.Name, err)
	}

	logrus.Infof("Successfully created volume %v.", volume.Name)
	return volume, nil
}

func doCreateVolume(stack *stack) error {
	// Doing find just to see if we are creating versus using an existing stack
	stc, err := stack.Find()
	if err != nil {
		return err
	}

	// Always run create because it also ensures that things are active
	if _, err := stack.Create(); err != nil {
		return err
	}

	// If env was nil then we created stack so we need to format
	if stc == nil {
		volumeName := stack.volumeConfig.Name
		dev := getDevice(volumeName)
		if err := waitForDevice(dev); err != nil {
			return err
		}

		if stack.volumeConfig.DontFormat {
			logrus.Infof("Skipping formatting for volume %v.", volumeName)
		} else {
			logrus.Infof("Formatting volume %v - %v", volumeName, dev)
			cmd := exec.Command("mkfs.ext4", "-F", dev)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("Error running mkfs command: %v", err)
			}
		}
	}

	return nil
}

func (d *StorageDaemon) Delete(name string) error {
	logrus.Infof("Deleting volume %v", name)
	return newStack(d, &volumeConfig{Name: name}).Delete()
}

func (d *StorageDaemon) Attach(name string) (string, error) {
	logrus.Infof("Attaching volume %v", name)
	if err := newStack(d, &volumeConfig{Name: name}).StartController(); err != nil {
		return "", fmt.Errorf("Error moving controller for volume %s: %s", name, err)
	}
	dev := getDevice(name)
	if err := waitForDevice(dev); err != nil {
		return "", err
	}
	return dev, nil
}

func (d *StorageDaemon) Detach(dev string) error {
	name := getVolName(dev)
	logrus.Infof("Detaching volume %v", name)
	return newStack(d, &volumeConfig{Name: name}).StopController()
}

func getDevice(volumeName string) string {
	return filepath.Join(util.DevDir, volumeName)
}

func getVolName(dev string) string {
	return filepath.Base(dev)
}

func waitForDevice(dev string) error {
	err := util.Backoff(5*time.Minute, fmt.Sprintf("Failed to find %s", dev), func() (bool, error) {
		if _, err := os.Stat(dev); err == nil {
			return true, nil
		}
		return false, nil
	})
	return err
}

type volumeStore struct {
	sync.RWMutex
	mdClient md.Client
	hostUUID string
	rootDir  string
}

type volumeConfig struct {
	Name             string `json:"name,omitempty"`
	Size             string `json:"size,omitempty"`
	SizeGB           string `json:"sizeGB,omitempty"`
	ReadIOPS         string `json:"readIops,omitempty"`
	WriteIOPS        string `json:"writeIops,omitempty"`
	ReplicaBaseImage string `json:"replicaBaseImage,omitempty"`
	DontFormat       bool   `json:"dontFormat,omitempty"`
}

func (v *volumeConfig) JSON() string {
	j, err := json.Marshal(v)
	if err != nil {
		logrus.Errorf("Error marshalling volume config %v: %v", v, err)
		return ""
	}

	return string(j)
}
