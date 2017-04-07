package api

import (
	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
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
	Endpoint            string `json:"endpoint,omitemtpy"`
	Created             string `json:"created,omitemtpy"`

	RecurringJobs []*types.RecurringJob `json:"recurringJobs,omitempty"`

	Replicas   []Replica   `json:"replicas,omitempty"`
	Controller *Controller `json:"controller,omitempty"`
}

type Snapshot struct {
	client.Resource

	Name        string   `json:"name"`
	Parent      string   `json:"parent"`
	Children    []string `json:"children"`
	Removed     bool     `json:"removed"`
	UserCreated bool     `json:"usercreated"`
	Created     string   `json:"created"`
	Size        string   `json:"size"`
}

type Host struct {
	client.Resource

	UUID    string `json:"uuid,omitempty"`
	Name    string `json:"name,omitempty"`
	Address string `json:"address,omitempty"`
}

type BackupVolume struct {
	client.Resource
	types.BackupVolumeInfo
}

type Backup struct {
	client.Resource
	types.BackupInfo
}

type Setting struct {
	client.Resource
	Name  string `json:"name"`
	Value string `json:"value"`
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

type SnapshotInput struct {
	Name string `json:"name,omitempty"`
}

type BackupInput struct {
	Name string `json:"name,omitempty"`
}

type ScheduleInput struct {
	Jobs []types.RecurringJob `json:"jobs,omitempty"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("snapshot", Snapshot{})
	schemas.AddType("attachInput", AttachInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("backup", Backup{})
	schemas.AddType("backupInput", BackupInput{})
	schemas.AddType("recurringJob", types.RecurringJob{})

	hostSchema(schemas.AddType("host", Host{}))
	volumeSchema(schemas.AddType("volume", Volume{}))
	backupVolumeSchema(schemas.AddType("backupVolume", BackupVolume{}))
	settingSchema(schemas.AddType("setting", Setting{}))
	scheduleSchema(schemas.AddType("scheduleInput", ScheduleInput{}))

	return schemas
}

func scheduleSchema(schedule *client.Schema) {
	jobs := schedule.ResourceFields["jobs"]
	jobs.Type = "array[recurringJob]"
	schedule.ResourceFields["jobs"] = jobs
}

func settingSchema(setting *client.Schema) {
	setting.CollectionMethods = []string{"GET"}
	setting.ResourceMethods = []string{"GET", "PUT"}

	settingName := setting.ResourceFields["name"]
	settingName.Required = true
	settingName.Unique = true
	setting.ResourceFields["name"] = settingName

	settingValue := setting.ResourceFields["value"]
	settingValue.Required = true
	settingValue.Update = true
	setting.ResourceFields["value"] = settingValue
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
			Input:  "attachInput",
			Output: "volume",
		},
		"detach": {
			Output: "volume",
		},
		"snapshotPurge": {},

		"snapshotCreate": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotGet": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotList": {},
		"snapshotDelete": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotRevert": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotBackup": {
			Input: "snapshotInput",
		},
		"scheduleUpdate": {
			Input: "scheduleInput",
		},
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

func backupVolumeSchema(backupVolume *client.Schema) {
	backupVolume.CollectionMethods = []string{"GET"}
	backupVolume.ResourceMethods = []string{"GET"}
	backupVolume.ResourceActions = map[string]client.Action{
		"backupList": {},
		"backupGet": {
			Input:  "backupInput",
			Output: "backup",
		},
		"backupDelete": {
			Input:  "backupInput",
			Output: "backupVolume",
		},
	}
}

func toSettingResource(name, value string) *Setting {
	return &Setting{
		Resource: client.Resource{
			Id:   name,
			Type: "setting",
		},
		Name:  name,
		Value: value,
	}
}

func toSettingCollection(settings *types.SettingsInfo) *client.GenericCollection {
	data := []interface{}{
		toSettingResource("backupTarget", settings.BackupTarget),
		toSettingResource("longhornImage", settings.LonghornImage),
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "setting"}}
}

func toVolumeResource(v *types.VolumeInfo, apiContext *api.ApiContext) *Volume {
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

	r := &Volume{
		Resource: client.Resource{
			Id:      v.Name,
			Type:    "volume",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:                v.Name,
		Size:                strconv.FormatInt(v.Size, 10),
		BaseImage:           v.BaseImage,
		FromBackup:          v.FromBackup,
		NumberOfReplicas:    v.NumberOfReplicas,
		State:               string(v.State),
		LonghornImage:       v.LonghornImage,
		RecurringJobs:       v.RecurringJobs,
		StaleReplicaTimeout: int(v.StaleReplicaTimeout / time.Minute),
		Endpoint:            v.Endpoint,
		Created:             v.Created,

		Controller: controller,
		Replicas:   replicas,
	}

	actions := map[string]struct{}{}

	switch v.State {
	case types.VolumeStateDetached:
		actions["attach"] = struct{}{}
		actions["scheduleUpdate"] = struct{}{}
	case types.VolumeStateHealthy:
		actions["detach"] = struct{}{}
		actions["snapshotPurge"] = struct{}{}
		actions["snapshotCreate"] = struct{}{}
		actions["snapshotList"] = struct{}{}
		actions["snapshotGet"] = struct{}{}
		actions["snapshotDelete"] = struct{}{}
		actions["snapshotRevert"] = struct{}{}
		actions["snapshotBackup"] = struct{}{}
		actions["scheduleUpdate"] = struct{}{}
	case types.VolumeStateDegraded:
		actions["detach"] = struct{}{}
		actions["snapshotPurge"] = struct{}{}
		actions["snapshotCreate"] = struct{}{}
		actions["snapshotList"] = struct{}{}
		actions["snapshotGet"] = struct{}{}
		actions["snapshotDelete"] = struct{}{}
		actions["snapshotRevert"] = struct{}{}
		actions["snapshotBackup"] = struct{}{}
	case types.VolumeStateCreated:
		actions["scheduleUpdate"] = struct{}{}
	case types.VolumeStateFaulted:
	}

	for action := range actions {
		r.Actions[action] = apiContext.UrlBuilder.ActionLink(r.Resource, action)
	}

	return r
}

func toVolumeCollection(vs []*types.VolumeInfo, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range vs {
		data = append(data, toVolumeResource(v, apiContext))
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
			Id:   s.Name,
			Type: "snapshot",
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

func toBackupVolumeResource(bv *types.BackupVolumeInfo, apiContext *api.ApiContext) *BackupVolume {
	if bv == nil {
		logrus.Warnf("weird: nil backupVolume")
		return nil
	}
	b := &BackupVolume{
		Resource: client.Resource{
			Id:    bv.Name,
			Type:  "backupVolume",
			Links: map[string]string{},
		},
		BackupVolumeInfo: *bv,
	}
	b.Actions = map[string]string{
		"backupList":   apiContext.UrlBuilder.ActionLink(b.Resource, "backupList"),
		"backupGet":    apiContext.UrlBuilder.ActionLink(b.Resource, "backupGet"),
		"backupDelete": apiContext.UrlBuilder.ActionLink(b.Resource, "backupDelete"),
	}
	return b
}

func toBackupVolumeCollection(bv []*types.BackupVolumeInfo, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range bv {
		data = append(data, toBackupVolumeResource(v, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backupVolume"}}
}

func toBackupResource(b *types.BackupInfo) *Backup {
	if b == nil {
		logrus.Warnf("weird: nil backup")
		return nil
	}
	return &Backup{
		Resource: client.Resource{
			Id:    b.Name,
			Type:  "backup",
			Links: map[string]string{},
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
