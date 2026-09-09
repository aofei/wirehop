package monotime

import "time"

// Time represents a protocol clock reading as a time value for local deadline arithmetic. Its epoch is arbitrary and
// must never be compared with wall time. Splitting seconds from microseconds avoids duration overflow for long uptimes.
func Time(micros uint64) time.Time {
	return time.Unix(int64(micros/1_000_000), int64(micros%1_000_000)*int64(time.Microsecond))
}
