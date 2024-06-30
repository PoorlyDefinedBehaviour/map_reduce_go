package clock

import "time"

type Clock struct{}

func New() Clock {
	return Clock{}
}

func (clock Clock) Now() time.Time {
	return time.Now()
}

func (clock Clock) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

func (clock Clock) Since(t time.Time) time.Duration {
	return time.Since(t)
}
