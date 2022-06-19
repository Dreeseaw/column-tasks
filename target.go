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

func (t *Target) Apply(deltaMap map[string]deltaSet) {
    if deltas, rowChange := deltaMap["row"]; rowChange {
        if deltas[0].Type == 1 {
            // process insert
            t.inner.Insert(func (r column.Row) error {
                for colName, vDelta := range deltaMap {
                    // overlook TTL for now
                    if colName != "row" && colName != "expire" {
                        r.SetAny(colName, vDelta[0].Payload)
                    }
                }
                return nil
            })
        } else {
            // process delete
            t.inner.DeleteAt(deltas[0].Offset)
        }
    } else {
        // process update
        t.inner.Query(func (txn *column.Txn) error {
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
}
