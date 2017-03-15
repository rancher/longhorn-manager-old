package api

import (
	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"strconv"
	"time"
)

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("attachInput", AttachInput{})

	volumeSchema(schemas.AddType("volume", Volume{}))

	return schemas
}

func volumeSchema(volume *client.Schema) {
	volume.CollectionMethods = []string{"GET", "POST"}
	volume.ResourceMethods = []string{"GET", "DELETE"}
	volume.ResourceActions = map[string]client.Action{
		"attach": {
			Input: "attachInput",
		},
		"detach": {},
	}
	volume.ResourceFields["controller"] = client.Field{
		Type:     "struct",
		Nullable: true,
	}
	volumeName := volume.ResourceFields["name"]
	volumeName.Create = true
	volumeName.Required = true
	volumeName.Unique = true
	volume.ResourceFields["name"] = volumeName

	volumeSize := volume.ResourceFields["size"]
	volumeSize.Create = true
	volumeSize.Required = true
	volumeSize.Default = "100G"
	volume.ResourceFields["size"] = volumeSize

	volumeNumberOfReplicas := volume.ResourceFields["numberOfReplicas"]
	volumeNumberOfReplicas.Create = true
	volumeNumberOfReplicas.Required = true
	volumeNumberOfReplicas.Default = 2
	volume.ResourceFields["numberOfReplicas"] = volumeNumberOfReplicas

	volumeStaleReplicaTimeout := volume.ResourceFields["staleReplicaTimeout"]
	volumeStaleReplicaTimeout.Create = true
	volumeStaleReplicaTimeout.Default = 20
	volume.ResourceFields["staleReplicaTimeout"] = volumeStaleReplicaTimeout
}

type Volume struct {
	client.Resource

	Name                string `json:"name,omitempty"`
	Size                string `json:"size,omitempty"`
	BaseImage           string `json:"baseImage,omitempty"`
	NumberOfReplicas    int    `json:"numberOfReplicas,omitempty"`
	StaleReplicaTimeout int    `json:"staleReplicaTimeout,omitempty"`
	State               string `json:"state,omitempty"`

	Replicas   []Replica   `json:"replicas,omitempty"`
	Controller *Controller `json:"controller,omitempty"`
}

type Instance struct {
	HostID  string `json:"hostId,omitempty"`
	Address string `json:"address,omitempty"`
	Running bool   `json:"running,omitempty"`
}

type Controller struct {
	Instance
}

type Replica struct {
	Instance

	Name         string `json:"name,omitempty"`
	Mode         string `json:"mode,omitempty"`
	BadTimestamp string `json:"badTimestamp,omitempty"`
}

type AttachInput struct {
	client.Resource

	HostID string `json:"hostId,omitempty"`
}

type Empty struct {
	client.Resource
}

var volumeState = map[types.VolumeState]string{
	types.Detached: "detached",
	types.Faulted:  "faulted",
	types.Healthy:  "healthy",
	types.Degraded: "degraded",
}

var replicaModes = map[types.ReplicaMode]string{
	types.RW:  "RW",
	types.WO:  "WO",
	types.ERR: "ERR",
}

func toVolumeResource(v *types.VolumeInfo) *Volume {
	state := ""
	if v.State != nil {
		state = volumeState[*v.State]
	}

	replicas := []Replica{}
	for _, r := range v.Replicas {
		mode := ""
		if r.Running {
			mode = replicaModes[r.Mode]
		}
		badTimestamp := ""
		if r.BadTimestamp != nil {
			badTimestamp = util.FormatTimeZ(*r.BadTimestamp)
		}
		replicas = append(replicas, Replica{
			Instance: Instance{
				Running: r.Running,
				Address: r.Address,
				HostID:  r.HostID,
			},
			Name:         r.Name,
			Mode:         mode,
			BadTimestamp: badTimestamp,
		})
	}

	var controller *Controller
	if v.Controller != nil {
		controller = &Controller{Instance{
			Running: v.Controller.Running,
			HostID:  v.Controller.HostID,
			Address: v.Controller.Address,
		}}
	}

	logrus.Debugf("controller: %+v", controller)

	return &Volume{
		Resource: client.Resource{
			Type: "volume",
			Actions: map[string]string{
				"attach": v.Name + "/attach",
				"detach": v.Name + "/detach",
			},
			Links: map[string]string{
				"self": v.Name,
			},
		},
		Name:                v.Name,
		Size:                strconv.FormatInt(v.Size, 10),
		NumberOfReplicas:    v.NumberOfReplicas,
		State:               state,
		StaleReplicaTimeout: int(v.StaleReplicaTimeout / time.Minute),
		Replicas:            replicas,
		Controller:          controller,
		// TODO BaseImage
	}
}

func toVolumeCollection(vs []*types.VolumeInfo) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range vs {
		data = append(data, toVolumeResource(v))
	}
	return &client.GenericCollection{Data: data}
}

func fromVolumeResMap(m map[string]interface{}) (*types.VolumeInfo, error) {
	v := new(Volume)
	if err := mapstructure.Decode(m, v); err != nil {
		return nil, errors.Wrapf(err, "error converting volume info '%+v'", m)
	}
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting size '%s'", v.Size)
	}
	return &types.VolumeInfo{
		Name:                v.Name,
		Size:                util.RoundUpSize(size),
		NumberOfReplicas:    v.NumberOfReplicas,
		StaleReplicaTimeout: time.Duration(v.StaleReplicaTimeout) * time.Minute,
	}, nil
}
