package timepkg

import "time"

// Timer the timer interface
type Timer interface {
	// Stop stop the timer and prevent the function to be invoked
	Stop() bool
	// Reset reset the timer
	Reset(d time.Duration) bool
}

// Time the main time interface
type Time interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
	Tick(d time.Duration) <-chan time.Time
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

func (realTimePkg) Tick(d time.Duration) <-chan time.Time {
	return time.Tick(d)
}
