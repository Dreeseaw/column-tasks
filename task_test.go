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
    targetObj := NewTarget(target)

    /*
    ex: multi stream input
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
    ex: a group by
    task := ... {
        t.Target("id") = t.Source("id")
        t.Target("cnt") = t.Source("cnt").GroupBy("id")
    }
    */

    /*
    ex: a simple numeric op
    task := ... {
        t.Target("id").Set( t.Source("id") )
        t.Target("cnt").Set( t.Source("cnt").Mult(3) )
    }
    */

    // replica task example
    task := CreateTask("mytask", stream, targetObj, func(t *Task) {
        idCol := t.Source("id")
        cntCol := t.Source("cnt")

        t.Target("id").Set(idCol)
        t.Target("cnt").Set(cntCol)
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
