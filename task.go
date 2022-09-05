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
    trgt   *Target // the target connected to the collection to be updated
    ops []Operation

    // Task Definition accessable
    Target map[string]*marker // let user assign output cols
}

// basic building block of task definitions
type marker struct {
    id    string
    path  []string
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    
    // connect this task to the upstream table's changes
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    // init Target & Source maps for definition reference
    tarMap := make(map[string]*Delta)
    // srcMap := make(map[string]*Delta)

    t := &Task{
        name:   id,
        srcChn: srcChan,
        colSrc: make(map[string]commChan),
        trgt:   trgt,
        ops:    make([]Operation, 0) // should be set
        Target: tarMap,
        // Source: srcMap,
    }

    // process task definiton
    def(t)
    if !len(t.Target) {
        return nil
    }

    // let's define the structure first (ex:)
    //    S.id  S.cnt
    //      |      |
    //    T.id   T.cnt

    // markers -> operations
    t.ops = append(t.ops, &RowOp{
        offsetMap: make(map[uint64]uint64),
    })
    for cName, finalMarker := range t.Target {
        t.ops = append(t.ops, &TargetOp{
            colName: cName,
        })
        for step, _ := range finalMarker.path {
            switch step {
            case "src":
                t.ops = append(t.ops, &SourceOp{
                    colName: finalMarker.id,
                })
            }
        }
    }

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
                if deltas[0].Type == 1 { // process insert
                    err := t.trgt.Insert(deltaMap)
                    if err != nil {
                        panic() // TODO: return errors
                    }
                    
                } else { // process delete
                    err := t.trgt.Delete(deltas[0].Offset)
                    if err != nil {
                        panic() // TODO: return errors
                    }
                }
            } else { // process update
                err := t.trgt.Update(deltaMap)
                if err != nil {
                    panic()
                }
            }

            // goal is to mock
            // t.target.inner.Replay(change)
        }
    }()

}

// --- The Task Definition API ---

func (t *Task) Source(colName string) *marker {
    nodeId := colName

    // TODO: validate source table has column

    // delta op saves task structure
    d := &marker{
        id: nodeId,
        path: make([]string, 0),
    }
    d.path = append(d.path, "src")
    return d
}
