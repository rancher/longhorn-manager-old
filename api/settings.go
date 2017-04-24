package api

import (
	"bytes"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-manager/eventlog"
	"github.com/rancher/longhorn-manager/types"
)

type SettingsHandlers struct {
	settings types.Settings
	man      types.VolumeManager
}

func (s *SettingsHandlers) List(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	si, err := s.settings.GetSettings()
	if err != nil && si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	apiContext.Write(toSettingCollection(si))
	return nil
}

func (s *SettingsHandlers) Get(w http.ResponseWriter, req *http.Request) error {
	name := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	si, err := s.settings.GetSettings()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}
	var value string
	switch name {
	case "backupTarget":
		value = si.BackupTarget
	case "engineImage":
		value = si.EngineImage
	case "syslogTarget":
		value = si.SyslogTarget
	default:
		return errors.Errorf("invalid setting name %v", name)
	}
	apiContext.Write(toSettingResource(name, value))
	return nil
}

func (s *SettingsHandlers) Set(w http.ResponseWriter, req *http.Request) error {
	var setting Setting

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&setting); err != nil {
		return err
	}

	name := mux.Vars(req)["name"]

	si, err := s.settings.GetSettings()
	if err != nil || si == nil {
		return errors.Wrap(err, "fail to read settings")
	}

	switch name {
	case "backupTarget":
		si.BackupTarget = setting.Value
	case "engineImage":
		si.EngineImage = setting.Value
	case "syslogTarget":
		si.SyslogTarget = setting.Value
	default:
		return errors.Wrapf(err, "invalid setting name %v", name)
	}
	if err := s.settings.SetSettings(si); err != nil {
		eventlog.Errorf("Error updating settings: %s to '%s'", name, setting.Value)
		return errors.Wrapf(err, "fail to set settings %v", si)
	}
	if name == "syslogTarget" {
		go s.updateEventLog(si.SyslogTarget)
	} else {
		eventlog.Infof("Updated settings: %s = '%s'", name, setting.Value)
	}

	apiContext.Write(toSettingResource(name, setting.Value))
	return nil
}

var httpClient = &http.Client{Timeout: 10 * time.Second}
var nilReader = bytes.NewReader(nil)

func (s *SettingsHandlers) updateEventLog(syslogTarget string) {
	if err := eventlog.Update(syslogTarget); err != nil {
		logrus.Warnf("%v", errors.Wrapf(err, "unable to update event logger, syslogTarget '%s'", syslogTarget))
		return
	}
	s.bumpEventLogOnAllHosts()
}

func (s *SettingsHandlers) bumpEventLogOnAllHosts() {
	hosts, err := s.man.ListHosts()
	if err != nil {
		logrus.Errorf("%+v", errors.Wrap(err, "unable to list hosts"))
		return
	}
	for _, host := range hosts {
		addr := host.Address
		go func() {
			if _, err := httpClient.Post("http://"+addr+"/v1/bumpEventLog", "application/json", nilReader); err != nil {
				logrus.Errorf("%+v", errors.Wrapf(err, "unable to bump event log on host '%s'", addr))
			}
		}()
	}
}
