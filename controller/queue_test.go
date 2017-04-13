package controller

import (
	"github.com/rancher/longhorn-manager/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestTaskQueue(t *testing.T) {
	assert := require.New(t)

	q := TaskQueue()
	t0 := &types.BgTask{}
	wgPut := &sync.WaitGroup{}
	wgPut.Add(1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		wgPut.Done()
		t1 := q.Take()
		assert.Equal(t0, t1)
		assert.Equal(int64(1), t1.Num)
	}()
	wgPut.Wait()
	q.Put(t0)
	wg.Wait()
}

func TestTaskQueue_Close(t *testing.T) {
	assert := require.New(t)

	q := TaskQueue()
	assert.Zero(len(q.List()))
	assert.NotNil(q.List())

	wgTake := &sync.WaitGroup{}
	wgTake.Add(1)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		wgTake.Wait()
		assert.Nil(q.Take())
		assert.Nil(q.List())
	}()
	q.Close()
	wgTake.Done()
	wg.Wait()
}
