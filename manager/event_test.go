package manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSend(t *testing.T) {
	assert := require.New(t)

	ch := make(chan Event)
	go func() {
		<-ch
	}()
	assert.True(Send(ch, TimeEvent()))

	close(ch)
	assert.False(Send(ch, TimeEvent()))
}
