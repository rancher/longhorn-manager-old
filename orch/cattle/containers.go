package cattle

import (
	"fmt"
	client "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

func dataDir(volume *types.VolumeInfo) string {
	return fmt.Sprintf("/volume/%s", volume.Name)
}

func (orc *cattleOrc) replicaContainer(volume *types.VolumeInfo, replica *types.ReplicaInfo) *client.Container {
	return &client.Container{
		Name:       replica.Name,
		ImageUuid:  fmt.Sprintf("docker:%s", orc.LonghornImage),
		EntryPoint: []string{"longhorn"},
		Command: []string{
			"replica",
			"--listen=0.0.0.0:9502",
			fmt.Sprintf("--size=%v", volume.Size),
			dataDir(volume),
		},
		DataVolumes: []string{
			dataDir(volume),
			"/var/lib/rancher/longhorn/backups:/var/lib/rancher/longhorn/backups",
		},
		Labels: map[string]interface{}{
			"io.rancher.scheduler.affinity:container_label_soft_ne": "io.rancher.longhorn.replica.volume=" + volume.Name,
			"io.rancher.longhorn.replica.volume":                    volume.Name,
			"io.rancher.longhorn.volume":                            volume.Name,
			//"io.rancher.scheduler.affinity:container_soft":          orc.containerUUID,
		},
		StartOnCreate: false,
	}
}

func (orc *cattleOrc) controllerContainer(volume *types.VolumeInfo) *client.Container {
	command := []string{"launch", "controller", "--listen=0.0.0.0:9501", "--frontend=tgt"}
	for _, replica := range volume.Replicas {
		command = append(command, "--replica="+util.ReplicaAddress(replica.Name, volume.Name))
	}
	command = append(command, volume.Name)
	return &client.Container{
		Name:       "controller",
		ImageUuid:  fmt.Sprintf("docker:%s", orc.LonghornImage),
		Command:    command,
		Privileged: true,
		DataVolumes: []string{
			"/dev:/host/dev",
			"/proc:/host/proc",
		},
		Labels: map[string]interface{}{
			"io.rancher.scheduler.affinity:container": orc.containerUUID,
			"io.rancher.longhorn.controller.volume":   volume.Name,
			"io.rancher.longhorn.volume":              volume.Name,
		},
		StartOnCreate: true,
	}
}
