package policy

import (
	"errors"
	"testing"

	"github.com/aofei/wirehop/internal/target"
)

func TestTargetSet(t *testing.T) {
	allowed := target.MustParse("wg.example.com:51820")
	set, err := NewTargetSet([]target.Endpoint{allowed, allowed})
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 1 || !set.Allows(allowed) {
		t.Fatalf("target set does not contain %v exactly", allowed)
	}
	if set.Allows(target.MustParse("wg.example.com:51821")) {
		t.Fatal("target set allowed a different port")
	}
	for _, targets := range [][]target.Endpoint{
		nil,
		{{}},
	} {
		if _, err := NewTargetSet(targets); !errors.Is(err, ErrEmptyTargetSet) && !errors.Is(err, ErrInvalidTarget) {
			t.Fatalf("NewTargetSet(%v) error = %v", targets, err)
		}
	}
}
