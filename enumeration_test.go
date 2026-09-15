package rados

import (
	"errors"
	"testing"
)

func TestObjectCursorEndpointsAndOwnership(t *testing.T) {
	left := Pool{id: 7}
	right := Pool{id: 8}
	namespaced := left.WithNamespace("space")
	begin := left.BeginObjectCursor()
	end := left.EndObjectCursor()
	if begin.IsEnd() || !end.IsEnd() {
		t.Fatalf("begin end=%t, end end=%t", begin.IsEnd(), end.IsEnd())
	}
	if comparison, err := CompareObjectCursors(begin, end); err != nil || comparison >= 0 {
		t.Fatalf("compare begin/end = %d, %v", comparison, err)
	}
	if _, err := CompareObjectCursors(ObjectCursor{}, begin); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero cursor error = %v", err)
	}
	if _, err := CompareObjectCursors(begin, right.BeginObjectCursor()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("cross-pool error = %v", err)
	}
	if _, err := CompareObjectCursors(begin, namespaced.BeginObjectCursor()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("cross-namespace error = %v", err)
	}
	if _, err := namespaced.SplitCursor(begin, namespaced.EndObjectCursor(), 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("cross-namespace split error = %v", err)
	}
	object, err := begin.hobject()
	if err != nil || !object.IsMin() {
		t.Fatalf("begin object = %+v, %v", object, err)
	}
}

func TestSplitCursorProducesOrderedBoundaries(t *testing.T) {
	pool := Pool{id: 7}
	begin := pool.BeginObjectCursor()
	end := pool.EndObjectCursor()
	boundaries, err := pool.SplitCursor(begin, end, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundaries) != 8 || boundaries[0] != begin || boundaries[len(boundaries)-1] != end {
		t.Fatalf("boundaries = %#v", boundaries)
	}
	for index := 1; index < len(boundaries); index++ {
		comparison, err := CompareObjectCursors(boundaries[index-1], boundaries[index])
		if err != nil || comparison >= 0 {
			t.Fatalf("boundary %d comparison = %d, %v", index, comparison, err)
		}
	}
	one, err := pool.SplitCursor(begin, end, 1)
	if err != nil || len(one) != 2 || one[0] != begin || one[1] != end {
		t.Fatalf("single partition = %#v, %v", one, err)
	}
}

func TestSplitCursorRejectsInvalidRanges(t *testing.T) {
	pool := Pool{id: 7}
	other := Pool{id: 8}
	tests := []struct {
		name       string
		begin, end ObjectCursor
		partitions uint32
	}{
		{name: "zero partitions", begin: pool.BeginObjectCursor(), end: pool.EndObjectCursor()},
		{name: "zero cursor", end: pool.EndObjectCursor(), partitions: 1},
		{name: "cross pool", begin: pool.BeginObjectCursor(), end: other.EndObjectCursor(), partitions: 1},
		{name: "reversed", begin: pool.EndObjectCursor(), end: pool.BeginObjectCursor(), partitions: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := pool.SplitCursor(test.begin, test.end, test.partitions); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
