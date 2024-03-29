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
    w := NewStream(map[string]string{
        "id": "string",
        "cnt": "int",
    })
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
        idCol := t.Source("id")
        cntCol := t.Source("cnt")

        t.Target["id"] = idCol
        t.Target["cnt"] = cntCol
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
    // Insert a value to be deleted in 1 second
    source.InsertObjectWithTTL(map[string]interface{}{
        "id": "bob2",
        "cnt": 4,
    }, 1 * time.Second)
    
    // Update some values
    source.Query(func (txn *column.Txn) error {
        cnt := txn.Int("cnt")
        id := txn.Any("id")
        txn.Range(func (i uint32) {
            cnt.Set(3)
            id.Set("bob3")
        })
        return nil
    })
    
    // Sleep thru TTL delete
    // Allow 1 second (for now) for view refresh
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
func TestDeltaTask(t *testing.T) {
    stream, source, target := defaultTestColls()
    targetObj := NewTarget(target)

    /*
    ex: a group by?
    task := ... {
        t.Target["id"] = t.Source("id")
        t.Target["cnt"] = t.Source("cnt").GroupBy("id")
    }
    */

    task := CreateTask("mytask", stream, targetObj, func(t *Task) {
        t.Target["id"] = t.Source("id")
        t.Target["cnt"] = t.Multiply(t.Source("cnt"), 2)
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
    // Insert a value to be deleted in 1 second
    source.InsertObjectWithTTL(map[string]interface{}{
        "id": "bob2",
        "cnt": 4,
    }, 1 * time.Second)
    
    // Update some values
    source.Query(func (txn *column.Txn) error {
        cnt := txn.Int("cnt")
        id := txn.Any("id")
        txn.Range(func (i uint32) {
            cnt.Set(3)
            id.Set("bob3")
        })
        return nil
    })
    
    // Sleep thru TTL delete
    // Allow 1 second (for now) for view refresh
    time.Sleep(2 * time.Second)
    target.Query(func (txn *column.Txn) error {
        assert.Equal(t, 3, txn.Count())
        cnt := txn.Int("cnt")
        id := txn.Any("id")
        txn.Range(func (i uint32) {
            actualId, _ := id.Get(); assert.Equal(t, "bob3", actualId)
            actualCnt, _ := cnt.Get(); assert.Equal(t, 6, actualCnt)
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
