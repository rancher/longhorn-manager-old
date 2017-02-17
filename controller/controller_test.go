package controller

import (
	"github.com/rancher/longhorn-orc/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseReplica(t *testing.T) {
	assert := require.New(t)

	s0 := `tcp://replica-79VrD86STQ.volume-qq:9502 RW   [volume-head-002.img volume-snap-9828ec27-65a9-4ace-9ac9-603e8b0b61db.img volume-snap-31f1222c-fc8a-4c1d-b61a-23124c3be558.img]`
	s1 := `tcp://replica-Vx2qPNcLQX.volume-qq:9502 RW   [volume-head-001.img volume-snap-9828ec27-65a9-4ace-9ac9-603e8b0b61db.img volume-snap-31f1222c-fc8a-4c1d-b61a-23124c3be558.img]`

	replica, chain, err := parseReplica(s0)
	_, chain1, err1 := parseReplica(s1)

	assert.Nil(err)
	assert.Nil(err1)
	assert.Equal("[volume-head-002.img volume-snap-9828ec27-65a9-4ace-9ac9-603e8b0b61db.img volume-snap-31f1222c-fc8a-4c1d-b61a-23124c3be558.img]", chain)
	assert.NotNil(replica)
	assert.Equal("tcp://replica-79VrD86STQ.volume-qq:9502", replica.Address)
	assert.Equal(types.RW, replica.Mode)

	assert.Equal("volume-snap-9828ec27-65a9-4ace-9ac9-603e8b0b61db.img volume-snap-31f1222c-fc8a-4c1d-b61a-23124c3be558.img", trimChain(chain))
	assert.Equal(trimChain(chain), trimChain(chain1))
}
