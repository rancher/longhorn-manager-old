package types

import (
	"io"
	"time"
)

type VolumeState string

const (
	VolumeStateNone     = VolumeState("")
	VolumeStateCreated  = VolumeState("created")
	VolumeStateDetached = VolumeState("detached")
	VolumeStateFaulted  = VolumeState("faulted")
	VolumeStateHealthy  = VolumeState("healthy")
	VolumeStateDegraded = VolumeState("degraded")
)

type ReplicaMode string

const (
	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)

type InstanceType string

const (
	InstanceTypeNone       = InstanceType("")
	InstanceTypeController = InstanceType("controller")
	InstanceTypeReplica    = InstanceType("replica")
)

type VolumeManager interface {
	Start() error
	Create(volume *VolumeInfo) (*VolumeInfo, error)
	Delete(name string) error
	Get(name string) (*VolumeInfo, error)
	List() ([]*VolumeInfo, error)
	Attach(name string) error
	Detach(name string) error
	UpdateRecurring(name string, jobs []*RecurringJob) error
	ReplicaRemove(volumeName, replicaName string) error

	ListHosts() (map[string]*HostInfo, error)
	GetHost(id string) (*HostInfo, error)

	CheckController(ctrl Controller, volume *VolumeInfo) error
	Cleanup(volume *VolumeInfo) error

	Controller(name string) (Controller, error)
	SnapshotOps(name string) (SnapshotOps, error)
	VolumeBackupOps(name string) (VolumeBackupOps, error)
	Settings() Settings
	ManagerBackupOps(backupTarget string) ManagerBackupOps

	ProcessSchedule(spec *ScheduleSpec, item *ScheduleItem) (*InstanceInfo, error)
}

type Settings interface {
	GetSettings() (*SettingsInfo, error)
	SetSettings(*SettingsInfo) error
}

type SnapshotOps interface {
	Create(name string, labels map[string]string) (string, error)
	List() ([]*SnapshotInfo, error)
	Get(name string) (*SnapshotInfo, error)
	Delete(name string) error
	Revert(name string) error
	Purge() error
}

type VolumeBackupOps interface {
	Backup(snapName, backupTarget string) error
	StartBackup(snapName, backupTarget string) error
	LatestBackupStatus() *BackupStatusInfo
	Restore(backup string) error
	DeleteBackup(backup string) error
}

type GetManagerBackupOps func(backupTarget string) ManagerBackupOps

type ManagerBackupOps interface {
	List(volumeName string) ([]*BackupInfo, error)
	Get(url string) (*BackupInfo, error)
	Delete(url string) error

	ListVolumes() ([]*BackupVolumeInfo, error)
	GetVolume(volumeName string) (*BackupVolumeInfo, error)
}

type Event interface{}

type Monitor interface {
	io.Closer
	CronCh() chan<- Event
}

type BeginMonitoring func(volume *VolumeInfo, man VolumeManager) Monitor

type GetController func(volume *VolumeInfo) Controller

type Controller interface {
	Name() string
	Endpoint() string
	GetReplicaStates() ([]*ReplicaInfo, error)
	AddReplica(replica *ReplicaInfo) error
	RemoveReplica(replica *ReplicaInfo) error

	SnapshotOps() SnapshotOps
	BackupOps() VolumeBackupOps
}

type Orchestrator interface {
	CreateVolume(volume *VolumeInfo) (*VolumeInfo, error) // creates volume metadata and prepare for volume
	DeleteVolume(volumeName string) error                 // removes volume metadata
	GetVolume(volumeName string) (*VolumeInfo, error)     // For non-existing volume, return (nil, nil)
	ListVolumes() ([]*VolumeInfo, error)
	MarkBadReplica(volumeName string, replica *ReplicaInfo) error // find replica by Address
	UpdateVolume(volume *VolumeInfo) error

	CreateController(volumeName, controllerName string, replicas map[string]*ReplicaInfo) (*ControllerInfo, error)
	CreateReplica(volumeName, replicaName string) (*ReplicaInfo, error)

	StartInstance(instance *InstanceInfo) (*InstanceInfo, error)
	StopInstance(instance *InstanceInfo) (*InstanceInfo, error)
	RemoveInstance(instance *InstanceInfo) (*InstanceInfo, error)

	ListHosts() (map[string]*HostInfo, error)
	GetHost(id string) (*HostInfo, error)

	Scheduler() Scheduler // return nil if not supported

	ServiceLocator
	Settings
}

type ServiceLocator interface {
	GetCurrentHostID() string
	GetAddress(hostID string) (string, error) // Return <host>:<port>
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget" mapstructure:"backupTarget"`
	EngineImage  string `json:"engineImage" mapstructure:"engineImage"`
}

type VolumeInfo struct {
	Name                string
	Size                int64
	BaseImage           string
	FromBackup          string
	NumberOfReplicas    int
	StaleReplicaTimeout time.Duration
	Controller          *ControllerInfo
	Replicas            map[string]*ReplicaInfo //key is replicaName
	State               VolumeState
	EngineImage         string
	Endpoint            string
	Created             string
	RecurringJobs       []*RecurringJob
}

type InstanceInfo struct {
	ID         string
	Type       InstanceType
	Name       string
	HostID     string
	Address    string
	Running    bool
	VolumeName string
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	Mode         ReplicaMode
	BadTimestamp time.Time
}

type SnapshotInfo struct {
	Name        string            `json:"name"`
	Parent      string            `json:"parent"`
	Children    []string          `json:"children"`
	Removed     bool              `json:"removed"`
	UserCreated bool              `json:"usercreated"`
	Created     string            `json:"created"`
	Size        string            `json:"size"`
	Labels      map[string]string `json:"labels"`
}

type HostInfo struct {
	UUID    string `json:"uuid"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type BackupInfo struct {
	Name            string `json:"name,omitempty"`
	URL             string `json:"url,omitempty"`
	SnapshotName    string `json:"snapshotName,omitempty"`
	SnapshotCreated string `json:"snapshotCreated,omitempty"`
	Created         string `json:"created,omitempty"`
	Size            string `json:"size,omitempty"`
	VolumeName      string `json:"volumeName,omitempty"`
	VolumeSize      string `json:"volumeSize,omitempty"`
	VolumeCreated   string `json:"volumeCreated,omitempty"`
}

type BackupStatusInfo struct {
	InProgress   bool   `json:"inProgress"`
	Err          error  `json:"err"`
	Snapshot     string `json:"snapshot"`
	BackupTarget string `json:"backupTarget"`
	Started      string `json:"started"`
}

type BackupVolumeInfo struct {
	Name    string `json:"name"`
	Size    string `json:"size"`
	Created string `json:"created"`
}

const (
	SnapshotTask = "snapshot"
	BackupTask   = "backup"
)

type RecurringJob struct {
	Name   string `json:"name,omitempty"`
	Cron   string `json:"cron,omitempty"`
	Task   string `json:"task,omitempty"`
	Retain int    `json:"retain,omitempty"`
}
