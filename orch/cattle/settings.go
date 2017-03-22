package cattle

import (
	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	client "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/types"
)

func (orc *cattleOrc) Get() *types.SettingsInfo {
	svc, err := orc.metadata.GetSelfService()
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "error getting settings"))
	}
	data := svc.Metadata["settings"]
	if data == nil {
		return &types.SettingsInfo{
			BackupTarget: "vfs:///var/lib/longhorn/backups/default",
		}
	}
	settings := new(types.SettingsInfo)
	if err := mapstructure.Decode(data, settings); err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "error parsing settings"))
	}
	return settings
}

func (orc *cattleOrc) Set(s *types.SettingsInfo) {
	svcMd, err := orc.metadata.GetSelfService()
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "error getting settings"))
	}
	coll, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{"uuid": svcMd.UUID}})
	if len(coll.Data) != 1 {
		logrus.Fatal("could not find the self service")
	}
	svc := &coll.Data[0]
	svc.Metadata["settings"] = s
	if _, err := orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	}); err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "error saving metadata"))
	}
}
