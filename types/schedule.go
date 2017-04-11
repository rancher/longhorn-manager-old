package types

const (
	ScheduleActionCreateController = "create-controller"
	ScheduleActionCreateReplica    = "create-replica"
	ScheduleActionDeleteInstance   = "delete"
	ScheduleActionStartInstance    = "start"
	ScheduleActionStopInstance     = "stop"
)

type Scheduler interface {
	Init(ops ScheduleOps)
	Schedule(item *ScheduleItem) (*InstanceInfo, error)
	Process(item *ScheduleItem) (*InstanceInfo, error)
}

type ScheduleOps interface {
	ProcessSchedule(item *ScheduleItem) (*InstanceInfo, error)
}

type ScheduleItem struct {
	Action   string
	Instance *ScheduleInstance
	Data     *ScheduleData
}

type ScheduleInstance struct {
	ID string
}

type ScheduleData struct {
	Orchestrator string
	Data         []byte // encoded orchestrator specific data
}
