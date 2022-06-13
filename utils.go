package tasks

import (
    "fmt"
    comm "github.com/kelindar/column/commit"
)

type Change struct {
    Column string
    Type comm.OpType
    Offset int32
    Payload any
}

func printChange(change comm.Commit) {
    fmt.Printf("\t----\n")
    reader := comm.NewReader()
    for _, u := range change.Updates {
        for reader.Seek(u); reader.Next(); {
            var payload any

            // get col type from Coll, switch on that
            switch u.Column {
            case "id":
                payload = reader.String()
            case "cnt":
                payload = reader.Int()
            default:
                payload = "special"
            }
            cc := Change{
                Column: u.Column,
                Type: reader.Type,
                Offset: reader.Offset,
                Payload: payload,
            }
            fmt.Printf("Change: %v\n", cc)
        }
    }
}
