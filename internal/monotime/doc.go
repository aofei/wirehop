// Package monotime exposes process-local serializable monotonic timestamps. Darwin and Linux clocks include time spent
// suspended. Other platforms follow the suspend behavior of Go's monotonic clock source.
package monotime
