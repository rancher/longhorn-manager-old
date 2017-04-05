package cattle

import (
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	client "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/types"
)

func (orc *cattleOrc) GetSettings() (*types.SettingsInfo, error) {
	svc, err := orc.metadata.GetSelfService()
	if err != nil {
		return nil, errors.Wrap(err, "error getting settings")
	}
	data := svc.Metadata["settings"]
	if data == nil {
		return &types.SettingsInfo{
			BackupTarget:  "vfs:///var/lib/longhorn/backups/default",
			LonghornImage: orc.LonghornImage,
		}, nil
	}
	settings := new(types.SettingsInfo)
	if err := mapstructure.Decode(data, settings); err != nil {
		return nil, errors.Wrap(err, "error parsing settings")
	}
	if settings.LonghornImage == "" {
		settings.LonghornImage = orc.LonghornImage
	}
	return settings, nil
}

func (orc *cattleOrc) SetSettings(s *types.SettingsInfo) error {
	svcMd, err := orc.metadata.GetSelfService()
	if err != nil {
		return errors.Wrap(err, "error getting settings")
	}
	coll, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{"uuid": svcMd.UUID}})
	if len(coll.Data) != 1 {
		return errors.New("could not find the self service")
	}
	svc := &coll.Data[0]
	svc.Metadata["settings"] = s
	if _, err := orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	}); err != nil {
		return errors.Wrap(err, "error saving metadata")
	}
	return nil
}
