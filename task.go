package tasks

import (
    "fmt"
    "time"

    "github.com/kelindar/column"
    comm "github.com/kelindar/column/commit"
)

// --- Stream ---

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

// --- Replica Task ---

// ReplicaTask is a simple task that replicates the changes from a source
// collection to a target collection
type ReplicaTask struct {
    source *Stream
    target *column.Collection
}

// CreateReplicaTask returns a task, ready to be started
func CreateReplicaTask(src *Stream, trgt *column.Collection) *ReplicaTask {
    return &ReplicaTask{
        source: src,
        target: trgt,
    }
}

// Start the task before making changes to collection
func (t *ReplicaTask) Start() {
    go func() {
        for change := range t.source.inner {
            t.target.Replay(change)
        }
    }()
}

// --- Stream Task ---

type StreamTaskFn func(c comm.Commit) error

// StreamTask executes a callback query whenever a source collection is updated in any way
type StreamTask struct {
    source *Stream
    target *column.Collection
    fn     StreamTaskFn
}

// CreateStreamTask returns the task, ready to be started
func CreateStreamTask(src *Stream, trgt *column.Collection, fn StreamTaskFn) *StreamTask {
    t := &StreamTask{
        source: src,
        target: trgt,
        fn: fn,
    }
    return t
}

// Start the task
func (t *StreamTask) Start() {
    if t.fn == nil {
        return
    }
    go func() {
        for change := range t.source.inner {
            // convert change to something readable?
            t.fn(change)
        }
    }()
}

// --- Cron Task ---

// CronTask runs a query on a collection at a specified time interval
type CronTask struct {
    source   *column.Collection
    fn       func(txn *column.Txn) error
    interval int // in ms
    stop     chan struct{}
}

// CreateCronTask is pretty simple
func CreateCronTask(src *column.Collection, fn func(txn *column.Txn) error) *CronTask {
    return &CronTask{
        source: src,
        fn: fn,
        interval: 1000,
        stop: make(chan struct{}),
    }
}

// Start the task
func (t *CronTask) Start() {
    ticker := time.NewTicker(time.Duration(t.interval) * time.Millisecond)

    go func() {
        for {
            select {
                case <-t.stop:
                    ticker.Stop()
                    return
                case t := <-ticker.C:
                    fmt.Printf("tick time: %v\n", t)
            }
        }
    }()

    fmt.Println("Cron Task stopped")
}

// Stop the task
func (t *CronTask) Stop() {
    t.stop <- struct{}{}
}
