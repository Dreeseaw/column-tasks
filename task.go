package tasks

import (
    "fmt"
    // "github.com/kelindar/column"
)

// --- Task ---

type TaskDef func(t *Task)

// Task forms a map of deltas (operations) and an execution plan upon creation,
// and then executes the plan on each set of changes onto the target collection
type Task struct {
    name     string // the id of the task
    srcChn   commChan // the stream connected to the source collection
    target   *Target // the target connected to the collection to be updated

    // task definition
    srcOps   map[string]*op // user-assigned input cols
    tarOps   map[string]*op // user-assigned output cols

    // execution & intermediate storage
    Stages   []*Stage
}

// CreateTask returns the task, ready to be started
func CreateTask(id string, src *Stream, trgt *Target, def TaskDef) *Task {
    
    // connect this task to the upstream table's changes
    srcChan := src.AddTask(id)
    if srcChan == nil {
        return nil
    }

    // init Target & Source maps for definition reference
    tarMap := make(map[string]*op)
    srcMap := make(map[string]*op)

    t := &Task{
        name: id,
        srcChn: srcChan,
        target: trgt,
        tarOps: tarMap,
        srcOps: srcMap,
        actMap: nil,
    }

    // process task definiton to op structure
    def(t)

    // process op structure to actor map
    t.compileOps()

    return t
}

// Start the task
func (t *Task) Start() {

    // start the source splitter
    go func() {
        for change := range t.srcChn {
            deltaMap := getDeltas(change)
            fmt.Println(deltaMap)
            
            // create new changes for target table via actors
            processedMap := t.executeActors(deltaMap)

            // apply generated changes to target table
            // TODO: abstract target to independent goroutine
            t.target.Apply(processedMap)
        }
    }()

}

func (t *Task) isSourceOp(o *op) (ret bool) {
    for _, sOp := range t.srcOps {
        if sOp == op {
            ret = true
            return
        }
    }
    return
}

func (t *Task) compileOps() {

    // eval op structure (via traversing from tar -> src)
    // into actors, actual processing components
    stages := make([]*Stage, 0)

    // create col-wise map for compiling down
    // colName -> list of ops
    colOpMap := make(map[string]opList)

    // start with all target ops
    eval := make([]*op, 0)
    for colName, tarOp := range t.tarOps {
        colOpList := make(opList, 0)
        colOpMap[colName] = append(colOpList, tarOp)
        eval = append(eval, tarOp)
    }

    // 'recursively' process linked list, create actors 
    // for len(eval) != 0 {
    stages = append(stages, &Stage{})
    for _, curOp := range eval {
        // add op to col's list in map
        ops := colOpMap[curOp.col]
        ops = append(ops, curOp)
        colOpMap[curOp.col] = ops

        // add prev, pop?
        // eval = append(eval[:curI], eval[curI+1:]...)
        if curOp.prev != nil {
            eval = append(eval, curOp.prev)
        }
    }
    // }

    stage1 := make(Stage, 0)
    for colName, colList := range colOpMap {
        switch len(colList) {
        case 1:
            // target col defined but not set
            // no op
        case 2:
            // direct load op
            stage1 = append(stage1, &BaseActor{
                col: colName
            })
        default:
            // actual op!
        }
    }
    stages = append(stages, stage1)
    t.Stages = stages

    return
}

func (t *Task) executeActors(deltaList deltaSet) deltaSet {
    
    tmpSet := make(deltaSet, 0)

    // actors within a stage are orderless
    for s, stage := range t.Stages {
        for a, actor := range stage {
            tmpSet = append(tmpSet, actor.pass(deltaList)...)
        }
        mergeDeltas(tmpSet)
    }

    return deltaMap
}

// --- The Task Definition API ---

type opList []*op

// op is the underlying data needed to define a task
type op struct {
    id   string
    kind string
    prev *op
    next *op
}

// Source returns an object representative of a column from the given source
// collection.
func (t *Task) Source(colName string) *op {

    // op gets dml for specific col
    d := &op{
        col: colName,
        kind: "src",
        prev: nil,
        next: nil,
    }
    t.srcOps[colName] = d
    return d
}

func (t *Task) Target(colName string) *op {
    d := &op{
        col: colName,
        kind: "tar",
        prev: nil,
        next: nil,
    }
    t.tarOps[colName] = d
    return d
}

func (o *op) Set(n *op) {
    o.prev = n
    n.next = o
}
