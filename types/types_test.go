package types

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTypes1(t *testing.T) {
	assert := require.New(t)

	assert.Equal(2, DefaultNumberOfReplicas)
	assert.IsType(time.Hour, DefaultStaleReplicaTimeout)
}
