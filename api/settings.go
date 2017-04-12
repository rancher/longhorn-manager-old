package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-manager/types"
)

type SettingsHandlers struct {
	settings types.Settings
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
	if name == "backupTarget" {
		apiContext.Write(toSettingResource(name, si.BackupTarget))
	} else if name == "longhornImage" {
		apiContext.Write(toSettingResource(name, si.LonghornImage))
	} else {
		return errors.Errorf("invalid setting name %v", name)
	}
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

	if name == "backupTarget" {
		si.BackupTarget = setting.Value
	} else if name == "longhornImage" {
		si.LonghornImage = setting.Value
	} else {
		return errors.Wrapf(err, "invalid setting name %v", name)
	}
	if err := s.settings.SetSettings(si); err != nil {
		return errors.Wrapf(err, "fail to set settings %v", si)
	}

	apiContext.Write(toSettingResource(name, setting.Value))
	return nil
}
