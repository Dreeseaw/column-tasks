package tasks

import (
    "github.com/kelindar/column"
    comm "github.com/kelindar/column/commit"
)

// type Stream commit.Channel
// type Table  *column.Collection
type TaskFn func()

type Task struct {
    source *Stream
    target *column.Collection
    fn     TaskFn
}

func CreateTask(src *Stream, trgt *column.Collection, fn TaskFn) *Task {
    t := &Task{
        source: src,
        target: trgt,
        fn: fn,
    }
    return t
}

func (t *Task) Start() {
    if t.fn != nil {
        return
    }
    go func() {
        for change := range t.source.inner {
            t.target.Replay(change)
        }
    }()
}

// Implements commit.Logger 
type Stream struct {
    inner chan comm.Commit
}

func NewStream() *Stream {
    return &Stream{
        inner: make(chan comm.Commit, 1024),
    }
}

func (s *Stream) Append(commit comm.Commit) error {
    s.inner <- commit.Clone()
    return nil
}
