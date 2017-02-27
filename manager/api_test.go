package manager

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestMap2VolumeInfo(t *testing.T) {
	assert := require.New(t)

	m := map[string]interface{}{
		"Name": "qq",
		"Size": "200g",
		"NumberOfReplicas": 2,
	}
	v, err := Map2VolumeInfo(m)
	assert.Nil(err)
	assert.Equal("qq", v.Name)
	assert.Equal(int64(214748364800), v.Size)
	assert.Equal(2, v.NumberOfReplicas)
}
