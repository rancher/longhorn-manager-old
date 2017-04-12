package api

import (
	"encoding/json"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-orc/types"
)

type ScheduleInput struct {
	Spec types.ScheduleSpec
	Item types.ScheduleItem
}

type ScheduleOutput struct {
	Instance types.InstanceInfo
}

func (s *Server) Schedule(rw http.ResponseWriter, req *http.Request) error {
	var input ScheduleInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "error read scheduleInput")
	}

	logrus.Debugf("Schedule request for %v %+v", input.Item.Action, input.Item.Instance)
	instance, err := s.man.ProcessSchedule(&input.Spec, &input.Item)
	if err != nil {
		return errors.Wrapf(err, "fail to execute %v %+v",
			input.Item.Action, input.Item.Instance)
	}
	output := ScheduleOutput{
		Instance: *instance,
	}
	json.NewEncoder(rw).Encode(output)
	return nil
}
