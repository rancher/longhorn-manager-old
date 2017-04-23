package kvstore

import (
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/types"
)

const (
	keyVolumes = "volumes"
)

func (s *KVStore) volumeKey(id string) string {
	return filepath.Join(s.key(keyVolumes), id)
}

func (s *KVStore) SetVolume(volume *types.VolumeInfo) error {
	if err := s.kvSet(s.volumeKey(volume.Name), volume); err != nil {
		return errors.Wrapf(err, "unable to set volume %+v", volume)
	}
	return nil
}

func (s *KVStore) GetVolume(id string) (*types.VolumeInfo, error) {
	volume, err := s.getVolumeByKey(s.volumeKey(id))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get volume %v", id)
	}
	return volume, nil
}

func (s *KVStore) getVolumeByKey(key string) (*types.VolumeInfo, error) {
	volume := types.VolumeInfo{}
	if err := s.kvGet(key, &volume); err != nil {
		if s.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return &volume, nil
}

func (s *KVStore) DeleteVolume(id string) error {
	if err := s.kvDelete(s.volumeKey(id), true); err != nil {
		return errors.Wrap(err, "unable to remove volume")
	}
	return nil
}

func (s *KVStore) ListVolumes() ([]*types.VolumeInfo, error) {
	volumeKeys, err := s.kvListKeys(s.key(keyVolumes))
	if err != nil {
		return nil, errors.Wrap(err, "unable to list volumes")
	}
	volumes := []*types.VolumeInfo{}
	for _, key := range volumeKeys {
		volume, err := s.getVolumeByKey(key)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to list volumes")
		}
		if volume != nil {
			volumes = append(volumes, volume)
		}
	}
	return volumes, nil
}
