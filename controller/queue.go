package controller

import (
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	"time"
)

type taskQueue struct {
	queue []*types.BgTask

	reqCh    chan interface{}
	takeReqs []takeReq
}

type listReq chan []*types.BgTask
type putReq *types.BgTask
type takeReq chan *types.BgTask

func (tq *taskQueue) runQueue() {
	var i int64
	for r := range tq.reqCh {
		switch r := r.(type) {
		case listReq:
			r <- tq.queue
		case putReq:
			i++
			r.Num = i
			r.Submitted = util.FormatTimeZ(time.Now())
			if len(tq.takeReqs) > 0 {
				tq.takeReqs[0] <- r
				tq.takeReqs = tq.takeReqs[1:]
			} else {
				tq.queue = append(tq.queue, r)
			}
		case takeReq:
			if len(tq.queue) > 0 {
				r <- tq.queue[0]
				tq.queue = tq.queue[1:]
			} else {
				tq.takeReqs = append(tq.takeReqs, r)
			}
		}
	}
	for _, r := range tq.takeReqs {
		close(r)
	}
}

func TaskQueue() types.TaskQueue {
	tq := &taskQueue{queue: []*types.BgTask{}, reqCh: make(chan interface{}), takeReqs: []takeReq{}}
	go tq.runQueue()
	return tq
}

func (tq *taskQueue) List() []*types.BgTask {
	defer func() {
		recover()
	}()
	req := make(listReq)
	tq.reqCh <- req
	return <-req
}

func (tq *taskQueue) Put(t *types.BgTask) {
	defer func() {
		recover()
	}()
	tq.reqCh <- putReq(t)
}

func (tq *taskQueue) Take() *types.BgTask {
	defer func() {
		recover()
	}()
	req := make(takeReq)
	tq.reqCh <- req
	return <-req
}

func (tq *taskQueue) Close() error {
	defer func() {
		recover()
	}()
	close(tq.reqCh)
	return nil
}
