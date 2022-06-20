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

func getDeltas(change comm.Commit) deltaSet {
    reader := comm.NewReader()
    colDeltas := make(deltaSet, 0)

    // fmt.Printf("\t----\n")
    for _, u := range change.Updates {
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
        // deltaSets[u.Column] = colDeltas
    }
    return colDeltas
}

func mergeDeltas(deltas deltaSet) deltaSet {
    // go thru deltas, delete row multiples

}
