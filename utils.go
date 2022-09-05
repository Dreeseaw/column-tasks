package tasks

import (
    // "fmt"
    comm "github.com/kelindar/column/commit"
)

// Offset -> delta
type deltaMap map[uint32]*delta 

// Represents changes to one offset
type delta struct {
    Type uint8 // 1 = insert
    Payload PayloadMap // For Inserts & Updates
}

// Column -> Value
type PayloadMap map[string]any

func getDeltas(change comm.Commit) deltaMap {
    reader := comm.NewReader()
    dMap := make(deltaMap)

    for _, u := range change.Updates {
        for reader.Seek(u); reader.Next(); {
            ofst := uint32(reader.Offset)

            // In the case of updates, mulitple 
            // offsets can be written to in 
            // one change
            d, exists := dMap[ofst]
            if !exists {
                del := &delta{
                    Type: 2, // Update by default - see below
                    Payload: make(PayloadMap), 
                }
                d = del
                dMap[ofst] = del
            }
            
            // Insert or Delete contain 'row' column
            if u.Column == "row" {
                d.Type = uint8(reader.Type)
            }

            // TODO: get col type from Coll, switch on that
            switch u.Column {
            case "id":
                d.Payload[u.Column] = reader.String()
            case "cnt":
                d.Payload[u.Column] = reader.Int()
            default:
                d.Payload[u.Column] = "unknown"
            }
        }
    }
    return dMap
}
