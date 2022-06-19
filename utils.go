package tasks

import (
    // "fmt"
    comm "github.com/kelindar/column/commit"
)

type delta struct {
    Column string
    Type comm.OpType
    Offset uint32
    Payload any
}

type deltaSet []delta

func getDeltas(change comm.Commit) map[string]deltaSet {
    reader := comm.NewReader()
    deltaSets := make(map[string]deltaSet)

    // fmt.Printf("\t----\n")
    for _, u := range change.Updates {
        colDeltas := make([]delta, 0)
        for reader.Seek(u); reader.Next(); {
            var payload any

            // get col type from Coll, switch on that
            switch u.Column {
            case "id":
                payload = reader.String()
            case "cnt":
                payload = reader.Int()
            case "row":
                payload = reader.Type
            default:
                payload = "special"
            }
            cc := delta{
                Column: u.Column,
                Type: reader.Type,
                Offset: uint32(reader.Offset),
                Payload: payload,
            }
            colDeltas = append(colDeltas, cc)
            // fmt.Printf("Change: %v\n", cc)
        }
        deltaSets[u.Column] = colDeltas
    }
    return deltaSets
}
