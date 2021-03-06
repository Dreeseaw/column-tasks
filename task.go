package tasks

import (
    "fmt"
    "github.com/kelindar/column"
)

// --- Task ---

type TaskDef func(t *Task)

// Task forms a map of deltas (operations) and an execution plan upon creation,
// and then executes the plan on each set of changes onto the target collection
type Task struct {
    // internal
    name   string // the id of the task
    srcChn commChan // the stream connected to the source collection
    colSrc map[string]commChan // split source into needed col delta nodes
    target *Target // the target connected to the collection to be updated

    // API accessable
    // Target map[string]*Delta // let user assign output cols
    // Source map[string]*Delta // let user assign input cols
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    
    // connect this task to the upstream table's changes
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    // init Target & Source maps for definition reference
    // tarMap := make(map[string]*Delta)
    // srcMap := make(map[string]*Delta)

    t := &Task{
        name: id,
        srcChn: srcChan,
        colSrc: make(map[string]commChan),
        target: trgt,
        // Target: tarMap,
        // Source: srcMap,
    }

    // process task definiton
    def(t)

    return t
}

// Start the task
func (t *Task) Start() {

    // start the source splitter
    go func() {
        for change := range t.srcChn {
            deltaMap := getDeltas(change)
            fmt.Println(deltaMap)
            
            if deltas, rowChange := deltaMap["row"]; rowChange {
                if deltas[0].Type == 1 {
                    // process insert
                    t.target.inner.Insert(func (r column.Row) error {
                        for colName, vDelta := range deltaMap {
                            // overlook TTL for now
                            if colName != "row" && colName != "expire" {
                                r.SetAny(colName, vDelta[0].Payload)
                            }
                        }
                        return nil
                    })
                } else {
                    // process delete
                    t.target.inner.DeleteAt(deltas[0].Offset)
                }
            } else {
                // process update
                t.target.inner.Query(func (txn *column.Txn) error {
                    for colName, uDeltas := range deltaMap {
                        for _, curDelta := range uDeltas {
                            txn.QueryAt(curDelta.Offset, func (r column.Row) error {
                                r.SetAny(colName, curDelta.Payload)
                                return nil
                            })
                        }
                    }
                    return nil
                })
            }

            // goal is to mock
            // t.target.inner.Replay(change)
        }
    }()

}

// --- The Task Definition API ---

/*
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
*/
