package tasks

import (
    "fmt"

//     "github.com/kelindar/column"
    mapset "github.com/deckarep/golang-set/v2"
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
    srcCols mapset.Set[string]
    // ops []Operation

    // Task Definition accessable
    Target map[string]*marker // let user assign output cols
}

// basic building block of task definitions
type marker struct {
    src   string
    path  []string
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    
    // connect this task to the upstream table's changes
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    // init Target maps for definition reference
    tarMap := make(map[string]*marker)

    t := &Task{
        name:   id,
        srcChn: srcChan,
        colSrc: make(map[string]commChan),
        trgt:   trgt,
        srcCols: mapset.NewSet[string](),
        // ops:    make([]Operation, 0), // should be set
        Target: tarMap,
        // Source: srcMap,
    }

    // process task definiton
    def(t)
    if len(t.Target) == 0 {
        return nil
    }

    // let's define the structure first (ex:)
    //    S.id  S.cnt
    //      |      |
    //    T.id   T.cnt

    // markers -> operations
    for _, finalMarker := range t.Target {
        for _, step := range finalMarker.path {
            if step ==  "src" {
                t.srcCols.Add(finalMarker.src)
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
            
            // Only need changes for relevant columns
            for _, del := range deltaMap {
                if del.Type == 0 {
                    continue
                }
                for cn, _ := range del.Payload {
                    if !t.srcCols.Contains(cn) {
                        delete(del.Payload, cn)
                    }
                }
            }


            for ofst, del := range deltaMap {
                if del.Type == 1 {
                    t.trgt.Insert(del.Payload) 
                } else if del.Type == 0 {
                    t.trgt.Delete(ofst)
                } else {
                    t.trgt.Update(ofst, del.Payload)
                }
            }
        }
    }()

}

// --- The Task Definition API ---

func (t *Task) Source(colName string) *marker {
    // TODO: validate source table has column

    // delta op saves task structure
    d := &marker{
        src: colName,
        path: make([]string, 0),
    }
    d.path = append(d.path, "src")
    return d
}
