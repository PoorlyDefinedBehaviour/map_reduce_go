package contracts

import "time"

type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
	Since(time.Time) time.Duration
}
