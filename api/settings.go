package api

import (
	"net/http"

	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-orc/types"
)

type SettingsHandlers struct {
	settings types.Settings
}

func (s *SettingsHandlers) Get(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	si := s.settings.GetSettings()
	if si == nil {
		return errors.Errorf("Cannot find settings")
	}
	apiContext.Write(toSettingsResource(si))
	return nil
}

func (s *SettingsHandlers) Set(w http.ResponseWriter, req *http.Request) error {
	var settings SettingsResource

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&settings); err != nil {
		return err
	}
	s.settings.SetSettings(&settings.SettingsInfo)

	apiContext.Write(settings)
	return nil
}
