package tasks

import (
//    "fmt"
)

// --- Delta Operation Node --- 

type Delta struct {
    id        string
    workQueue commChan
}

// --- Task ---

type TaskDef func(t *Task)

// Task forms a map of deltas (operations) and an execution plan upon creation,
// and then executes the plan on each set of changes onto the target collection
type Task struct {
    name   string // the id of the task
    source commChan // the stream connected to the source collection
    colSrc map[string]commChan // split source into needed col delta nodes
    target *Target // the target connected to the collection to be updated
    deltas map[string]*Delta // map of all delta nodes used in task

    // API accessable
    Target map[string]*Delta // let user assign upstream delta op
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    t := &Task{
        name: id,
        source: srcChan,
        colSrc: make(map[string]commChan),
        target: trgt,
        deltas: make(map[string]*Delta),
        Target: make(map[string]*Delta),
    }

    // process task definiton
    def(t)

    return t
}

// Start the task
func (t *Task) Start() {

    // start the source splitter
    go func() {
        for change := range t.source {
            printChange(change)
        }
    }()

}

// --- The Task Definition API ---

func (t *Task) Source(colName string) *Delta {
    colChan := make(commChan, 1024)
    t.colSrc[colName] = colChan
    nodeId := colName + "_src"

    // delta op gets dml for specific col
    d := &Delta{
        id: nodeId,
        workQueue: colChan,
    }
    t.deltas[nodeId] = d
    return d
}
