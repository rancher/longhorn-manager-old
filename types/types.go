package types

import (
	"io"
	"time"
)

const (
	DefaultNumberOfReplicas    = 2
	DefaultStaleReplicaTimeout = time.Hour * 16 * 24
)

type VolumeState int

const (
	Detached VolumeState = iota
	Faulted
	Healthy
	Degraded
)

type ReplicaMode int

const (
	RW ReplicaMode = iota
	WO
	ERR
)

type VolumeManager interface {
	Create(volume *VolumeInfo) (*VolumeInfo, error)
	Delete(name string) error
	Get(name string) (*VolumeInfo, error)
	List() ([]*VolumeInfo, error)
	Attach(name string) error
	Detach(name string) error

	CheckController(ctrl Controller, volume *VolumeInfo) error
	Cleanup(volume *VolumeInfo) error

	Controller(name string) (Controller, error)
	VolumeSnapshots(name string) (VolumeSnapshots, error)
	VolumeBackups(name string) (VolumeBackups, error)
	Settings() Settings
	Backups(backupTarget string) Backups
}

type Settings interface {
	Get() *SettingsInfo
	Set(*SettingsInfo)
}

type VolumeSnapshots interface {
	Create(name string) (string, error)
	List() ([]*SnapshotInfo, error)
	Get(name string) (*SnapshotInfo, error)
	Delete(name string) error
	Revert(name string) error
}

type VolumeBackups interface {
	Backup(snapName, backupTarget string) error
	Restore(backup string) error
}

type GetBackups func(backupTarget string) Backups

type Backups interface {
	List(volumeName string) ([]*BackupInfo, error)
	Get(url string) (*BackupInfo, error)
	Delete(url string) error
}

type Monitor func(volume *VolumeInfo, man VolumeManager) io.Closer

type GetController func(volume *VolumeInfo) Controller

type Controller interface {
	Name() string
	GetReplicaStates() ([]*ReplicaInfo, error)
	AddReplica(replica *ReplicaInfo) error
	RemoveReplica(replica *ReplicaInfo) error

	Snapshots() VolumeSnapshots
	Backups() VolumeBackups
}

type Orchestrator interface {
	CreateVolume(volume *VolumeInfo) (*VolumeInfo, error) // creates replicas and volume metadata
	DeleteVolume(volumeName string) error                 // removes all the volume components
	GetVolume(volumeName string) (*VolumeInfo, error)
	MarkBadReplica(volumeName string, replica *ReplicaInfo) error // find replica by Address

	CreateController(volumeName string, replicas map[string]*ReplicaInfo) (*ControllerInfo, error)
	CreateReplica(volumeName string) (*ReplicaInfo, error)

	StartInstance(instanceID string) error
	StopInstance(instanceID string) error

	RemoveInstance(instanceID string) error

	GetThisHostID() string
}

type ServiceLocator interface {
	GetAddress(q string) (string, error)
	IsLocal(q string) bool
}

type SettingsInfo struct {
	BackupTarget string `json:"backupTarget" mapstructure:"backupTarget"`
}

type VolumeInfo struct {
	Name                string
	Size                int64
	BaseImage           string
	FromBackup          string
	NumberOfReplicas    int
	StaleReplicaTimeout time.Duration
	Controller          *ControllerInfo
	Replicas            map[string]*ReplicaInfo
	State               *VolumeState
}

type InstanceInfo struct {
	ID      string
	HostID  string
	Address string
	Running bool
}

type ControllerInfo struct {
	InstanceInfo
}

type ReplicaInfo struct {
	InstanceInfo

	Name         string
	Mode         ReplicaMode
	BadTimestamp *time.Time
}

type SnapshotInfo struct {
	Name        string   `json:"name,omitempty"`
	Parent      string   `json:"parent,omitempty"`
	Children    []string `json:"children,omitempty"`
	Removed     bool     `json:"removed,omitempty"`
	UserCreated bool     `json:"usercreated,omitempty"`
	Created     string   `json:"created,omitempty"`
	Size        string   `json:"size,omitempty"`
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
