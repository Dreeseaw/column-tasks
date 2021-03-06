package tasks

import (
//     "fmt"
    "time"
    "testing"

    "github.com/kelindar/column"
//    "github.com/kelindar/column/commit"
    "github.com/stretchr/testify/assert"
)

func defaultTestColls() (*Stream, *column.Collection, *column.Collection) {
    w := NewStream()
    source := column.NewCollection(column.Options{
        Writer: w,
    })
    source.CreateColumn("id", column.ForString())
    source.CreateColumn("cnt", column.ForInt())

    target := column.NewCollection()
    target.CreateColumn("id", column.ForString())
    target.CreateColumn("cnt", column.ForInt())

    return w, source, target
}

func TestReplicaTask(t *testing.T) {
    stream, source, target := defaultTestColls()

    stream.AddTask("replica")
    task := CreateReplicaTask(stream, target)
    task.Start()

    source.Insert(func (r column.Row) error {
        r.SetAny("id", "bob")
        r.SetInt("cnt", 2)
        return nil
    })

    time.Sleep(100 * time.Millisecond)
    target.Query(func (txn *column.Txn) error {
        assert.Equal(t, 1, txn.Count())
        return nil
    })
}

func TestTask(t *testing.T) {
    stream, source, target := defaultTestColls()
    targetObj := NewTarget(target)

    /*
    // v1
    task := CreateStreamTask(stream, target, func (src *Stream, tgt *Target) error {
        idCol := src.Col("id")
        cntCol := src.Col("cnt")

        tgt.Col("id") = idCol
        tgt.Col("cnt") = cntCol
        
        return nil
    })
    */

    /*
    v2
    task := CreateTask("mytask", stream, targetObj, func(t *Task) {
        idCol := t.Source("id")
        cntCol := t.Source("cnt")

        // t.Target("id") = idCol
        // t.Target("cnt") = cntCol
        t.Target("id").Set(idCol)
        t.Target("cnt").Set(cntCol)
    })
    */

    /*
    v4?
    task := CreateTask("mytask", stream, target, func(t *Task) {
        ex: replica
        t.Target["id"] = t.Source["id"]
        t.Target["cnt"] = t.Source["cnt"]

        ex: delta op
        ...
        t.Target["cnt"] = t.Source["cnt"].Multiply(2)
    })
    */

    /*
    multi stream input ex
    streams := map[string]*Stream{}{
        "tableA": stream1,
        "tableB": stream2,
    }
    task := CreateTask("mytask", streams, target, func(t *Task) {
        ex: simple join
        a_id := t.Source["tableA.id"]
        b_id := t.Source["tableB.id"]
        t.Target["id"] = a_id.Join(b_id)

        // need to distingiush join key?
        // use data from prev join?
        t.Target["cnt"] = t.Source["tableA.cnt"].Add(t.Source["tableB.cnt"])
    })
    */

    /*
    ex: a group by?
    task := ... {
        t.Target["id"] = t.Source("id")
        t.Target["cnt"] = t.Source("cnt").GroupBy("id")
    }
    */

    /*
    ex: a delta op?
    task := ... {
        t.Target["id"] = t.Source("id")
        t.Target["cnt"] = t.Source("cnt").Mult(3) 
    }
    */

    // v3
    task := CreateTask("mytask", stream, targetObj, func(t *Task) {
        //idCol := t.Source["id"]
        //cntCol := t.Source["cnt"]

        //t.Target["id"] = idCol
        //t.Target["cnt"] = cntCol
    })
    task.Start()

    i := 0
    for uint32(i) < uint32(1) + 2 {
        i++
        source.Insert(func (r column.Row) error {
            r.SetAny("id", "bob")
            r.SetInt("cnt", i)
            return nil
        })
    }
    source.InsertObjectWithTTL(map[string]interface{}{
        "id": "bob2",
        "cnt": 4,
    }, 1 * time.Second)
    
    source.Query(func (txn *column.Txn) error {
        cnt := txn.Int("cnt")
        id := txn.Any("id")
        txn.Range(func (i uint32) {
            cnt.Set(3)
            id.Set("bob3")
        })
        return nil
    })
    

    time.Sleep(2 * time.Second)
    target.Query(func (txn *column.Txn) error {
        assert.Equal(t, 3, txn.Count())
        cnt := txn.Int("cnt")
        id := txn.Any("id")
        txn.Range(func (i uint32) {
            actualId, _ := id.Get(); assert.Equal(t, "bob3", actualId)
            actualCnt, _ := cnt.Get(); assert.Equal(t, 3, actualCnt)
        })
        return nil
    })
}

// --- Replica Task ---
// mostly just an example 

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
        for change := range t.source.tasks["replica"] {
            t.target.Replay(change)
        }
    }()
}
