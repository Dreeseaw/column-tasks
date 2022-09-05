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

func (t *Target) Delete(offset uint64) error {
    return t.inner.DeleteAt(offset)
}

// TODO: also return inserted offset
func (t *Target) Insert(deltaMap map[string]deltaSet) error {
    return t.inner.Insert(func (r column.Row) error {
        for colName, vDelta := range deltaMap {
            // overlook TTL for now
            if colName != "row" && colName != "expire" {
                r.SetAny(colName, vDelta[0].Payload)
            }
        }
        return nil
    })
}

func (t *Target) Update(deltaMap map[string]deltaSet) error {
    return t.inner.Query(func (txn *column.Txn) error {
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
