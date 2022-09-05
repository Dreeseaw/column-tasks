package tasks

type Operation interface {
}

type RowOp struct {
    offsetMap map[uint64]uint64 
}

type SourceOp struct {
    colName string
}

type TargetOp struct {
    colName string
}

type MathOp struct {
}
