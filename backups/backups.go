package backups

import (
	"bytes"
	"encoding/json"
	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/controller"
	"github.com/rancher/longhorn-orc/types"
	"io"
	"os/exec"
	"strings"
)

type backups struct {
	BackupTarget string
}

type backupVolume struct {
	Name           string
	Size           string
	Created        string
	LastBackupName string
	SpaceUsage     string
	Backups        map[string]interface{}
}

func New(backupTarget string) types.ManagerBackupOps {
	return &backups{backupTarget}
}

func parseBackup(v interface{}) (*types.BackupInfo, error) {
	backup := new(types.BackupInfo)
	if err := mapstructure.Decode(v, backup); err != nil {
		return nil, errors.Wrapf(err, "Error parsing backup info %+v", v)
	}
	return backup, nil
}

func parseBackupsList(stdout io.Reader, volumeName string) ([]*types.BackupInfo, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]*backupVolume{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing backups: \n%s", buffer)
	}
	backups := []*types.BackupInfo{}
	if currentBackup := controller.CurrentBackup(); currentBackup != nil {
		backups = append(backups, currentBackup)
	}
	for _, v := range data[volumeName].Backups {
		backup, err := parseBackup(v)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}

	return backups, nil
}

func parseBackupVolumesList(stdout io.Reader) ([]*types.BackupVolumeInfo, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]*backupVolume{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing backups: \n%s", buffer)
	}
	volumes := []*types.BackupVolumeInfo{}

	for name, v := range data {
		volumes = append(volumes, &types.BackupVolumeInfo{
			Name:    name,
			Size:    v.Size,
			Created: v.Created,
		})
	}

	return volumes, nil
}

func parseOneBackup(stdout io.Reader) (*types.BackupInfo, error) {
	buffer := new(bytes.Buffer)
	reader := io.TeeReader(stdout, buffer)
	data := map[string]interface{}{}
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(buffer.String())), "cannot find ") {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error parsing backups: \n%s", buffer)
	}
	return parseBackup(data)
}

func (b *backups) ListVolumes() ([]*types.BackupVolumeInfo, error) {
	cmd := exec.Command("longhorn", "backup", "ls", "--volume-only", b.BackupTarget)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	return parseBackupVolumesList(stdout)
}

func (b *backups) GetVolume(volumeName string) (*types.BackupVolumeInfo, error) {
	cmd := exec.Command("longhorn", "backup", "ls", "--volume", volumeName, "--volume-only", b.BackupTarget)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	list, err := parseBackupVolumesList(stdout)
	if err != nil {
		return nil, err
	}
	return list[0], nil
}

func (b *backups) List(volumeName string) ([]*types.BackupInfo, error) {
	if volumeName == "" {
		return nil, nil
	}
	cmd := exec.Command("longhorn", "backup", "ls", "--volume", volumeName, b.BackupTarget)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	return parseBackupsList(stdout, volumeName)
}

func (b *backups) Get(url string) (*types.BackupInfo, error) {
	cmd := exec.Command("longhorn", "backup", "inspect", url)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	return parseOneBackup(stdout)
}

func (b *backups) Delete(url string) error {
	cmd := exec.Command("longhorn", "backup", "rm", url)
	errBuff := new(bytes.Buffer)
	cmd.Stderr = errBuff
	out, err := cmd.Output()
	if err != nil {
		s := string(out)
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(s)), "cannot find ") {
			logrus.Warnf("delete: could not find the backup: '%s'", url)
			return nil
		}
		return errors.Wrapf(err, "Error deleting backup: %s", errBuff)
	}
	return nil
}
