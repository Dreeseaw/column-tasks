package tasks

import (
    "github.com/kelindar/column"
)

// --- Target ---

// Target is a wrapper around a collection, that applies the changes generated
// by connected tasks
type Target struct {
    inner *column.Collection
}

func NewTarget(coll *column.Collection) *Target {
    return &Target{
        inner: coll,
    }
}
