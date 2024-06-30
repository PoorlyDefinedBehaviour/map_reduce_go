package clock

import "time"

type MockClock struct {
	now time.Time
}

func NewMock() *MockClock {
	return &MockClock{
		now: time.Now(),
	}
}

func (clock *MockClock) Now() time.Time {
	return clock.now
}

func (clock *MockClock) Sleep(duration time.Duration) {
	panic("todo")
}

func (clock *MockClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}
