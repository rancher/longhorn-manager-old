package manager

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/backups"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	"github.com/robfig/cron"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	JobName   = "job"
	BackupJob = "backupJob"

	retainBackupSnapshots = 2
)

type taskCons func(runner *jobRunner, job *types.RecurringJob, si *types.SettingsInfo) Task

var tasks = map[string]taskCons{
	types.SnapshotTaskName: SnapshotTask,
	types.BackupTaskName:   BackupTask,
}

type jobRunner struct {
	volume   *types.VolumeInfo
	ctrl     types.Controller
	settings types.Settings
}

func newJobRunner(volume *types.VolumeInfo, ctrl types.Controller, settings types.Settings) *jobRunner {
	return &jobRunner{volume: volume, ctrl: ctrl, settings: settings}
}

type cronUpdate []*types.RecurringJob

func CronUpdate(jobs []*types.RecurringJob) types.Event {
	return cronUpdate(jobs)
}

func RunJobs(volume *types.VolumeInfo, ctrl types.Controller, settings types.Settings, ch chan types.Event) {
	runner := newJobRunner(volume, ctrl, settings)

	c := runner.setJobs(volume.RecurringJobs)
	if c == nil {
		return
	}
	c.Start()
	defer func() {
		c.Stop()
	}()

	for e := range ch {
		switch e := e.(type) {
		case cronUpdate:
			c.Stop()
			c = runner.setJobs(e)
			if c == nil {
				return
			}
			c.Start()
			logrus.Infof("restarted recurring jobs, volume '%s'", volume.Name)
		}
	}
}

func ValidateJobs(jobs []*types.RecurringJob) error {
	c := cron.NewWithLocation(time.UTC)
	for _, j := range jobs {
		if t := tasks[j.Task]; t != nil {
			if strings.TrimSpace(j.Name) != j.Name || j.Name == "" {
				return errors.Errorf("job name cannot be empty, start or end with whitespace: '%s'", j.Name)
			}
			if _, ok := tasks[j.Task]; !ok {
				return errors.Errorf("invalid task '%s'", j.Task)
			}
			if err := c.AddFunc(j.Cron, func() {}); err != nil {
				return errors.Wrap(err, "cron job validation error")
			}
		}
	}
	return nil
}

func (runner *jobRunner) setJobs(jobs []*types.RecurringJob) *cron.Cron {
	si, err := runner.settings.GetSettings()
	if err != nil {
		logrus.Errorf("%+v", errors.Wrap(err, "unable to get settings, not setting jobs"))
		return nil
	}
	c := cron.NewWithLocation(time.UTC)
	for _, job := range jobs {
		if t := tasks[job.Task]; t != nil {
			c.AddFunc(job.Cron, runner.newTask(job, t(runner, job, si)))
			logrus.Infof("scheduled recurring job %+v, volume '%s'", job, runner.volume.Name)
		}
	}
	return c
}

func snapName(name string) string {
	return name + "-" + util.FormatTimeZ(time.Now()) + "-" + util.RandomID()
}

func (runner *jobRunner) newTask(job *types.RecurringJob, task Task) func() {
	return func() {
		if err := task.Run(); err != nil {
			logrus.Errorf("error running job: %+v", errors.Wrapf(err, "unable to run a task for job '%s'", job.Name))
			return
		}
	}
}

type Task interface {
	Run() error
}

type snapshotTask struct {
	sync.Mutex

	runner *jobRunner
	job    *types.RecurringJob

	count  int
	cached []*types.SnapshotInfo
}

func SnapshotTask(runner *jobRunner, job *types.RecurringJob, _ *types.SettingsInfo) Task {
	return &snapshotTask{runner: runner, job: job}
}

func (st *snapshotTask) Run() error {
	name := snapName(st.job.Name)
	logrus.Infof("recurring job: snapshot '%s', volume '%s'", name, st.runner.volume.Name)
	if _, err := st.runner.ctrl.SnapshotOps().Create(name, map[string]string{JobName: st.job.Name}); err != nil {
		return errors.Wrapf(err, "error running recurring job: snapshot '%s', volume '%s'", name, st.runner.volume.Name)
	}
	return st.cleanup()
}

func (st *snapshotTask) filterSnapshots(l []*types.SnapshotInfo) []*types.SnapshotInfo {
	r := []*types.SnapshotInfo{}
	for _, s := range l {
		if !s.Removed && s.Labels[JobName] == st.job.Name {
			r = append(r, s)
		}
	}
	return r
}

func (st *snapshotTask) listSnapshots() ([]*types.SnapshotInfo, error) {
	ss, err := st.runner.ctrl.SnapshotOps().List()
	if err != nil {
		return nil, errors.Wrapf(err, "error listing snapshots, volume '%s'", st.runner.volume.Name)
	}
	ss = st.filterSnapshots(ss)
	sort.Slice(ss, func(i, j int) bool { return ss[i].Created < ss[j].Created })
	return ss, nil
}

func (st *snapshotTask) cleanup() error {
	if st.job.Retain == 0 {
		return nil
	}

	st.Lock()
	defer st.Unlock()

	st.count++
	for st.cached == nil || st.count > st.job.Retain {
		if len(st.cached) == 0 {
			ss, err := st.listSnapshots()
			if err != nil {
				return errors.Wrapf(err, "error cleaning up snapshots, recurring job '%s', volume '%s'", st.job.Name, st.runner.volume.Name)
			}
			st.cached = ss
			st.count = len(ss)
		}
		for st.count > st.job.Retain && len(st.cached) > 0 {
			toRm := st.cached[0]
			logrus.Infof("recurring job cleanup: snapshot '%s', volume '%s'", toRm.Name, st.runner.volume.Name)
			if err := st.runner.ctrl.SnapshotOps().Delete(toRm.Name); err != nil {
				return errors.Wrapf(err, "deleting snapshot '%s', volume '%s'", toRm.Name, st.runner.volume.Name)
			}
			st.cached = st.cached[1:]
			st.count--
		}
		if err := st.runner.ctrl.SnapshotOps().Purge(); err != nil {
			return errors.Wrapf(err, "fail to purge snapshots when cleanup volume '%s'", st.runner.volume.Name)
		}
	}
	return nil
}

func BackupTask(runner *jobRunner, job *types.RecurringJob, si *types.SettingsInfo) Task {
	return &backupTask{runner: runner, job: job, backupTarget: si.BackupTarget}
}

type backupTask struct {
	sync.Mutex

	backupTarget string

	runner *jobRunner
	job    *types.RecurringJob

	count  int
	cached []*types.BackupInfo

	countSnapshots  int
	cachedSnapshots []*types.SnapshotInfo
}

func (bt *backupTask) Run() error {
	name := snapName(bt.job.Name)
	if _, err := bt.runner.ctrl.SnapshotOps().Create(name, map[string]string{JobName: bt.job.Name, BackupJob: bt.job.Name}); err != nil {
		return errors.Wrapf(err, "error creating snapshot for recurring backup '%s', volume '%s'", name, bt.runner.volume.Name)
	}
	bt.runner.ctrl.BgTaskQueue().Put(&types.BgTask{Task: types.BackupBgTask{
		Snapshot:     name,
		BackupTarget: bt.backupTarget,
		CleanupHook:  bt.cleanup,
	}})
	return nil
}

func (bt *backupTask) filterSnapshots(l []*types.SnapshotInfo) []*types.SnapshotInfo {
	r := []*types.SnapshotInfo{}
	for _, s := range l {
		if !s.Removed && s.Labels[JobName] == bt.job.Name && s.Labels[BackupJob] == bt.job.Name {
			r = append(r, s)
		}
	}
	return r
}

func (bt *backupTask) filterBackups(l []*types.BackupInfo) []*types.BackupInfo {
	r := []*types.BackupInfo{}
	for _, b := range l {
		if strings.HasPrefix(b.SnapshotName, bt.job.Name+"-") {
			r = append(r, b)
		}
	}
	return r
}

func (bt *backupTask) listSnapshots() ([]*types.SnapshotInfo, error) {
	ss, err := bt.runner.ctrl.SnapshotOps().List()
	if err != nil {
		return nil, errors.Wrapf(err, "error listing snapshots, volume '%s'", bt.runner.volume.Name)
	}
	ss = bt.filterSnapshots(ss)
	sort.Slice(ss, func(i, j int) bool { return ss[i].Created < ss[j].Created })
	return ss, nil
}

func (bt *backupTask) listBackups() ([]*types.BackupInfo, error) {
	backupOps := backups.New(bt.backupTarget)
	bs, err := backupOps.List(bt.runner.volume.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "error listing backups, volume '%s'", bt.runner.volume.Name)
	}
	bs = bt.filterBackups(bs)
	sort.Slice(bs, func(i, j int) bool { return bs[i].Created < bs[j].Created })
	return bs, nil
}

func (bt *backupTask) cleanup() error {
	if err := bt.cleanupBackupSnapshots(); err != nil {
		logrus.Errorf("%+v", errors.Wrap(err, "error cleaning up backup snapshots"))
	}
	return bt.cleanupBackups()
}

func (bt *backupTask) cleanupBackups() error {
	if bt.job.Retain == 0 {
		return nil
	}

	bt.Lock()
	defer bt.Unlock()

	bt.count++
	for bt.cached == nil || bt.count > bt.job.Retain {
		if len(bt.cached) == 0 {
			bs, err := bt.listBackups()
			if err != nil {
				return errors.Wrapf(err, "error cleaning up snapshots, recurring job '%s', volume '%s'", bt.job.Name, bt.runner.volume.Name)
			}
			bt.cached = bs
			bt.count = len(bs)
		}
		for bt.count > bt.job.Retain && len(bt.cached) > 0 {
			toRm := bt.cached[0]
			logrus.Infof("recurring job cleanup: backup '%s', volume '%s'", toRm.URL, bt.runner.volume.Name)
			if err := bt.runner.ctrl.BackupOps().DeleteBackup(toRm.URL); err != nil {
				return errors.Wrapf(err, "deleting backup '%s', volume '%s'", toRm.Name, bt.runner.volume.Name)
			}
			bt.cached = bt.cached[1:]
			bt.count--
		}
	}
	return nil
}

func (bt *backupTask) cleanupBackupSnapshots() error {
	bt.Lock()
	defer bt.Unlock()

	bt.countSnapshots++
	for bt.cachedSnapshots == nil || bt.countSnapshots > retainBackupSnapshots {
		if len(bt.cachedSnapshots) == 0 {
			ss, err := bt.listSnapshots()
			if err != nil {
				return errors.Wrapf(err, "error cleaning up snapshots, recurring job '%s', volume '%s'", bt.job.Name, bt.runner.volume.Name)
			}
			bt.cachedSnapshots = ss
			bt.countSnapshots = len(ss)
		}
		for bt.countSnapshots > retainBackupSnapshots && len(bt.cachedSnapshots) > 0 {
			toRm := bt.cachedSnapshots[0]
			logrus.Infof("recurring job cleanup: backup snapshot '%s', volume '%s'", toRm.Name, bt.runner.volume.Name)
			if err := bt.runner.ctrl.SnapshotOps().Delete(toRm.Name); err != nil {
				return errors.Wrapf(err, "deleting snapshot '%s', volume '%s'", toRm.Name, bt.runner.volume.Name)
			}
			bt.cachedSnapshots = bt.cachedSnapshots[1:]
			bt.countSnapshots--
		}
	}
	return nil
}
