package api

import (
	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"net/http"
	"strconv"
	"time"
)

type Volume struct {
	client.Resource

	Name                string `json:"name,omitempty"`
	Size                string `json:"size,omitempty"`
	BaseImage           string `json:"baseImage,omitempty"`
	FromBackup          string `json:"fromBackup,omitempty"`
	NumberOfReplicas    int    `json:"numberOfReplicas,omitempty"`
	StaleReplicaTimeout int    `json:"staleReplicaTimeout,omitempty"`
	State               string `json:"state,omitempty"`
	LonghornImage       string `json:"longhornImage,omitempty"`

	Replicas   []Replica   `json:"replicas,omitempty"`
	Controller *Controller `json:"controller,omitempty"`
}

type Snapshot struct {
	client.Resource

	Name        string   `json:"name,omitempty"`
	Parent      string   `json:"parent,omitempty"`
	Children    []string `json:"children,omitempty"`
	Removed     bool     `json:"removed,omitempty"`
	UserCreated bool     `json:"usercreated,omitempty"`
	Created     string   `json:"created,omitempty"`
	Size        string   `json:"size,omitempty"`
}

type Host struct {
	client.Resource

	UUID    string `json:"uuid,omitempty"`
	Name    string `json:"name,omitempty"`
	Address string `json:"address,omitempty"`
}

type Backup struct {
	client.Resource
	types.BackupInfo
}

type SettingsResource struct {
	client.Resource
	types.SettingsInfo
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

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("attachInput", AttachInput{})

	hostSchema(schemas.AddType("host", Host{}))
	volumeSchema(schemas.AddType("volume", Volume{}))
	snapshotSchema(schemas.AddType("snapshot", Snapshot{}))
	backupSchema(schemas.AddType("backup", Backup{}))
	settingsSchema(schemas.AddType("settings", SettingsResource{}))

	return schemas
}

func settingsSchema(settings *client.Schema) {
	settings.CollectionMethods = []string{}
	settings.ResourceMethods = []string{"GET", "PUT"}

	backupTarget := settings.ResourceFields["backupTarget"]
	backupTarget.Update = true
	backupTarget.Required = true
	settings.ResourceFields["backupTarget"] = backupTarget

	longhornImage := settings.ResourceFields["longhornImage"]
	longhornImage.Update = true
	longhornImage.Required = true
	settings.ResourceFields["longhornImage"] = longhornImage
}

func hostSchema(host *client.Schema) {
	host.CollectionMethods = []string{"GET"}
	host.ResourceMethods = []string{"GET"}
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

	volumeFromBackup := volume.ResourceFields["fromBackup"]
	volumeFromBackup.Create = true
	volume.ResourceFields["fromBackup"] = volumeFromBackup

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

func snapshotSchema(snapshot *client.Schema) {
	snapshot.CollectionMethods = []string{"GET", "POST"}
	snapshot.ResourceMethods = []string{"GET", "DELETE"}
	snapshot.ResourceActions = map[string]client.Action{
		"revert": {},
		"backup": {},
	}

	snapshotName := snapshot.ResourceFields["name"]
	snapshotName.Create = true
	snapshotName.Unique = true
	snapshot.ResourceFields["name"] = snapshotName
}

func backupSchema(backup *client.Schema) {
	backup.CollectionMethods = []string{"GET"}
	backup.ResourceMethods = []string{"GET", "DELETE"}
	backup.ResourceActions = map[string]client.Action{}
}

func toSettingsResource(s *types.SettingsInfo) *SettingsResource {
	return &SettingsResource{
		Resource: client.Resource{
			Type: "settings",
		},
		SettingsInfo: *s,
	}
}

func toVolumeResource(v *types.VolumeInfo) *Volume {
	replicas := []Replica{}
	for _, r := range v.Replicas {
		mode := ""
		if r.Running {
			mode = string(r.Mode)
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
			Id:   v.Name,
			Type: "volume",
			Actions: map[string]string{
				"attach": v.Name + "/attach",
				"detach": v.Name + "/detach",
			},
			Links: map[string]string{
				"snapshots": v.Name + "/snapshots/",
			},
		},
		Name:                v.Name,
		Size:                strconv.FormatInt(v.Size, 10),
		BaseImage:           v.BaseImage,
		FromBackup:          v.FromBackup,
		NumberOfReplicas:    v.NumberOfReplicas,
		State:               string(v.State),
		LonghornImage:       v.LonghornImage,
		StaleReplicaTimeout: int(v.StaleReplicaTimeout / time.Minute),
		Replicas:            replicas,
		Controller:          controller,
	}
}

func toVolumeCollection(vs []*types.VolumeInfo) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range vs {
		data = append(data, toVolumeResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "volume"}}
}

func toSnapshotResource(s *types.SnapshotInfo) *Snapshot {
	if s == nil {
		logrus.Warn("weird: nil snapshot")
		return nil
	}
	return &Snapshot{
		Resource: client.Resource{
			Type: "snapshot",
			Actions: map[string]string{
				"revert": s.Name + "/revert",
				"backup": s.Name + "/backup",
			},
			Links: map[string]string{
				"self": s.Name,
			},
		},
		Name:        s.Name,
		Parent:      s.Parent,
		Children:    s.Children,
		Removed:     s.Removed,
		UserCreated: s.UserCreated,
		Created:     s.Created,
		Size:        s.Size,
	}
}

func toSnapshotCollection(ss []*types.SnapshotInfo) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range ss {
		data = append(data, toSnapshotResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "snapshot"}}
}

func fromSnapshotResMap(m map[string]interface{}) (*types.SnapshotInfo, error) {
	s := new(Snapshot)
	if err := mapstructure.Decode(m, s); err != nil {
		return nil, errors.Wrapf(err, "error converting snapshot info '%+v'", m)
	}
	return &types.SnapshotInfo{
		Name: s.Name,
	}, nil
}

func toHostCollection(hosts map[string]*types.HostInfo) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range hosts {
		data = append(data, toHostResource(v))
	}
	return &client.GenericCollection{Data: data}
}

func toHostResource(h *types.HostInfo) *Host {
	return &Host{
		Resource: client.Resource{
			Id:      h.UUID,
			Type:    "host",
			Actions: map[string]string{},
		},
		UUID:    h.UUID,
		Name:    h.Name,
		Address: h.Address,
	}
}

func toBackupResource(b *types.BackupInfo) *Backup {
	if b == nil {
		logrus.Warnf("weird: nil backup")
		return nil
	}
	return &Backup{
		Resource: client.Resource{
			Type: "backup",
			Links: map[string]string{
				"self": b.Name + "?volume=" + b.VolumeName,
			},
		},
		BackupInfo: *b,
	}
}

func toBackupCollection(bs []*types.BackupInfo) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range bs {
		data = append(data, toBackupResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backup"}}
}

func fromSettingsResMap(m map[string]interface{}) (*types.SettingsInfo, error) {
	s := new(types.SettingsInfo)
	if err := mapstructure.Decode(m, s); err != nil {
		return nil, errors.Wrapf(err, "error converting settings info '%+v'", m)
	}
	return s, nil
}

type Server struct {
	man       types.VolumeManager
	sl        types.ServiceLocator
	proxy     http.Handler
	fwd       *Fwd
	snapshots *SnapshotHandlers
	settings  *SettingsHandlers
	backups   *BackupsHandlers
}

func NewServer(m types.VolumeManager, sl types.ServiceLocator, proxy http.Handler) *Server {
	return &Server{
		man:   m,
		sl:    sl,
		proxy: proxy,
		fwd:   &Fwd{sl, proxy},
		snapshots: &SnapshotHandlers{
			m,
		},
		settings: &SettingsHandlers{
			m.Settings(),
		},
		backups: &BackupsHandlers{
			m,
		},
	}
}
