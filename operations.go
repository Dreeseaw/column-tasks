package tasks

type Operation interface {
    Process(deltaMap, map[string]string)
}

type MultiplyOp struct {
    val any
    src string
}

func (mo MultiplyOp) Process(dMap deltaMap, schema map[string]string) {
    for _, del := range dMap {
        if del.Type == 1 {
            continue
        }
        for cN, cV := range del.Payload {
            if cN == mo.src {
                del.Payload[cN] = any_mult(cV, mo.val)
            }
        }
    }
}

func any_mult(v1, v2 any) any {
    switch v := v1.(type) {
    case int:
        return any(v * v2.(int))
    case float32:
        return any(v * v2.(float32))
    default:
        return 0
    }
}

/*
type AddColsOp struct {
    
}

func (aco *AddColsOp) Process(dMap deltaMap, schema map[string]string) {
    
}
*/
