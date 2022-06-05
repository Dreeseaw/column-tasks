package tasks

import (
    "github.com/kelindar/column"
    "github.com/kelindar/column/commit"
)

type Stream commit.Channel
// type Table  *column.Collection
type TaskFn func()

type Task struct {
    source commit.Channel
    target *column.Collection
    fn     TaskFn
}

func CreateTask(src commit.Channel, trgt *column.Collection, fn TaskFn) *Task {
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
        for change := range t.source {
            t.target.Replay(change)
        }
    }()
}
