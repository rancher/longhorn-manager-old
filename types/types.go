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
	Created VolumeState = iota
	Detached
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

	VolumeSnapshots(name string) (VolumeSnapshots, error)
}

type VolumeSnapshots interface {
	Create(name string) (string, error)
	List() ([]*SnapshotInfo, error)
	Get(name string) (*SnapshotInfo, error)
	Delete(name string) error
	Revert(name string) error
}

type Monitor func(volume *VolumeInfo, man VolumeManager) io.Closer

type GetController func(volume *VolumeInfo) Controller

type Controller interface {
	Name() string
	GetReplicaStates() ([]*ReplicaInfo, error)
	AddReplica(replica *ReplicaInfo) error
	RemoveReplica(replica *ReplicaInfo) error

	Snapshots() VolumeSnapshots
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

	ServiceLocator
}

type ServiceLocator interface {
	GetAddress(q string) (string, error)
	IsLocal(q string) bool
}

type VolumeInfo struct {
	Name                string
	Size                int64
	NumberOfReplicas    int
	StaleReplicaTimeout time.Duration
	Controller          *ControllerInfo
	Replicas            map[string]*ReplicaInfo
	State               VolumeState
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
