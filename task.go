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
    src    *Stream // the source object
    srcCols mapset.Set[string]
    ops []Operation

    // Task Definition accessable
    Target map[string]marker // let user assign output cols
}

// basic building block of task definitions
type marker interface {
    GetInps() []marker
}

type Marker struct {
    inps []marker
}

func (m Marker) GetInps() []marker {
    return m.inps
}

type srcMarker struct {
    Marker
    src   string
}

type mulMarker struct {
    Marker
    val   any
}

type addColsMarker struct {
    Marker
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    
    // connect this task to the upstream table's changes
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    // init Target maps for definition reference
    tarMap := make(map[string]marker)

    t := &Task{
        name:   id,
        srcChn: srcChan,
        colSrc: make(map[string]commChan),
        trgt:   trgt,
        src:    src,
        srcCols: mapset.NewSet[string](),
        ops:    make([]Operation, 0),
        Target: tarMap,
    }

    // process task definiton (create markers
    // via api funcs)
    def(t)
    if len(t.Target) == 0 {
        return nil
    }

    // markers -> operations (no optimization for now)
    markerQueue := make([]marker, 0)
    for _, finalMarker := range t.Target {
        markerQueue = append(markerQueue, finalMarker)
    }
    for len(markerQueue) > 0 {
        var m marker
        m, markerQueue = markerQueue[0], markerQueue[1:]
        for _, inpMarker := range m.GetInps() {
            markerQueue = append(markerQueue, inpMarker)
        }
        switch sm := m.(type) {
        case srcMarker:
            t.srcCols.Add(sm.src)
        case mulMarker:
            t.ops = append(t.ops, &MultiplyOp{
                val: sm.val,
                src: "cnt", //TODO: fix this absolute hack
            })
        default:
            panic("You may not use custom markers (yet).")
        }
    }

    /*
    for _, finalMarker := range t.Target {
        for _, op := range finalMarker.ops {
            // good place to develop dependency graph
            t.ops = append(t.ops, op)
        }
    }
    */

    return t
}

func (t *Task) SrcFilter(dMap deltaMap) {
    for ofst, del := range dMap {
        if del.Type == 0 {
            continue
        }
        for cn, _ := range del.Payload {
            if !t.srcCols.Contains(cn) {
                delete(del.Payload, cn)
            }
        }
        if len(del.Payload) == 0 {
            delete(dMap, ofst)
        }
    }
}

// Start the task
func (t *Task) Start() {

    // start the source splitter
    go func() {
        for change := range t.srcChn {
            dMap := getDeltas(change, t.src.schema)
            fmt.Println(dMap)
            
            // Only need changes for relevant columns
            // If this disqualifies an entire delta,
            // delete it from being processed
            t.SrcFilter(dMap)

            // create filter(f_col, f_val) API func,
            // filter out deltas with P[f_col] = f_val
            
            // create math API funcs
            for _, op := range t.ops {
                op.Process(dMap, t.src.schema)
            }

            // Apply the remaining deltas
            for ofst, del := range dMap {
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

func (t *Task) Source(colName string) marker {
    // Validate source table has column
    if _, exists := t.src.schema[colName]; !exists {
        return nil
    }

    // marker saves task structure
    return srcMarker{
        Marker: Marker{
            inps: nil,
        },
        src: colName,
    }
}

func (t *Task) Multiply(m marker, val any) marker {
    if m == nil {
        return nil
    }
    
    return mulMarker{
        Marker: Marker{
            inps: []marker{m},
        },
        val: val,
    }
}

/*
func (t *Task) AddCols(markers ...*marker) *marker {
    m := &AddColsMarker{
        inp: 
    }
    return m
}
*/
