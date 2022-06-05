package tasks

import (
    "time"
    "testing"

    "github.com/kelindar/column"
    "github.com/kelindar/column/commit"
    "github.com/stretchr/testify/assert"
)

func defaultTestColls() (commit.Channel, *column.Collection, *column.Collection) {
    w := make(commit.Channel, 1024)
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

func TestTask(t *testing.T) {
    stream, source, target := defaultTestColls()

    task := CreateTask(stream, target, nil)

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
