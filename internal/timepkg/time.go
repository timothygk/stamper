package timepkg

import "time"

// Timer the timer interface
type Timer interface {
	// Stop stop the timer and prevent the function to be invoked
	Stop() bool
	// Reset reset the timer
	Reset(d time.Duration) bool
}

type Ticker interface {
	// Chan the channel to read time
	Chan() <-chan time.Time
	// Stop stop the ticker
	Stop()
}

type realTicker struct {
	ticker *time.Ticker
}

func (t realTicker) Chan() <-chan time.Time {
	return t.ticker.C
}

func (t realTicker) Stop() {
	t.ticker.Stop()
}

// Time the main time interface
type Time interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
	Tick(d time.Duration) Ticker
}

// NewTime create a new Time
func NewTime() Time {
	return realTimePkg{}
}

type realTimePkg struct{}

func (realTimePkg) Now() time.Time {
	return time.Now()
}

func (realTimePkg) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}

func (realTimePkg) Tick(d time.Duration) Ticker {
	return realTicker{time.NewTicker(d)}
}
