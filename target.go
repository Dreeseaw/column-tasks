package tasks

import (
    "github.com/kelindar/column"
)

// --- Target ---

// Target is a wrapper around a collection, that applies the changes generated
// by connected tasks
type Target struct {
    inner *column.Collection
}

func NewTarget(coll *column.Collection) *Target {
    return &Target{
        inner: coll,
    }
}

func (t *Target) Delete(offset uint32) bool {
    return t.inner.DeleteAt(offset)
}

func (t *Target) Insert(payload map[string]any) (uint32, error) {
    return t.inner.Insert(func (r column.Row) error {
        for colName, colVal := range payload {
            // overlook TTL for now
            if colName != "row" && colName != "expire" {
                r.SetAny(colName, colVal)
            }
        }
        return nil
    })
}

func (t *Target) Update(ofst uint32, payload map[string]any) error {
    return t.inner.Query(func (txn *column.Txn) error {
        for colName, colVal := range payload {
            txn.QueryAt(ofst, func (r column.Row) error {
                r.SetAny(colName, colVal)
                return nil
            })
        }
        return nil
    })
}
