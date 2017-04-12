package manager

import (
	"github.com/rancher/longhorn-manager/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSend(t *testing.T) {
	assert := require.New(t)

	ch := make(chan types.Event)
	go func() {
		<-ch
	}()
	assert.True(Send(ch, TimeEvent()))

	close(ch)
	assert.False(Send(ch, TimeEvent()))
}
