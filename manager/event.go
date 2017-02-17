package manager

import (
	"time"
)

type Event interface{}

type event struct{}

func TimeEvent() Event {
	return &event{}
}

type Ticker interface {
	Start() Ticker
	Stop() Ticker
	NewTick() Event
}

type tickerImpl struct {
	ch       chan Event
	interval time.Duration
	timer    *time.Timer
}

func NewTicker(interval time.Duration, ch chan Event) Ticker {
	return &tickerImpl{interval: interval, ch: ch}
}

func (t *tickerImpl) Start() Ticker {
	if t.timer == nil {
		t.timer = time.NewTimer(t.interval)
		go t.tick()
	} else {
		t.timer.Reset(t.interval)
	}
	return t
}

func (t *tickerImpl) tick() {
	<-t.timer.C
	if Send(t.ch, t.NewTick()) {
		t.timer.Reset(t.interval)
		go t.tick()
	}
}

func (t *tickerImpl) NewTick() Event {
	return TimeEvent()
}

func (t *tickerImpl) Stop() Ticker {
	if t.timer != nil {
		t.timer.Stop()
	}
	return t
}

func Send(c chan<- Event, e Event) bool {
	if c == nil {
		return false
	}
	defer func() {
		recover() // otherwise, c <- e will panic if c is closed
	}()
	c <- e
	return true
}
