// Package policy implements server-side relay authorization policy.
package policy

import (
	"errors"

	"github.com/aofei/wirehop/internal/target"
)

var (
	// ErrInvalidTarget indicates a noncanonical, missing, or zero-port UDP target.
	ErrInvalidTarget = errors.New("invalid UDP target")
	// ErrEmptyTargetSet indicates a server policy that permits no explicit targets.
	ErrEmptyTargetSet = errors.New("empty target allowlist")
)

// TargetSet is an immutable exact-match logical target allowlist.
type TargetSet struct {
	targets map[target.Endpoint]struct{}
}

// NewTargetSet validates and copies an exact target allowlist.
func NewTargetSet(targets []target.Endpoint) (TargetSet, error) {
	if len(targets) == 0 {
		return TargetSet{}, ErrEmptyTargetSet
	}
	set := TargetSet{targets: make(map[target.Endpoint]struct{}, len(targets))}
	for _, endpoint := range targets {
		if !endpoint.Valid() {
			return TargetSet{}, ErrInvalidTarget
		}
		set.targets[endpoint] = struct{}{}
	}
	return set, nil
}

// Allows reports whether target is an exact member of the allowlist.
func (s TargetSet) Allows(endpoint target.Endpoint) bool {
	_, ok := s.targets[endpoint]
	return ok
}

// Len returns the number of distinct allowed targets.
func (s TargetSet) Len() int {
	return len(s.targets)
}
