package tasks

import (
    // "fmt"
)

// --- Actors ---

type ActorMap map[string]*Actor

type Actor interface {
    pass(deltas deltaSet) deltaSet
}

type BaseActor struct {
    col string
}

func (ba *BaseActor) pass(deltas deltaSet) deltaSet {
    ret := make(deltaSet, 0)
    for _, del := range deltas {
        if del.Column == "row" || del.Column == ba.col {
            ret = append(ret, del)
        }
    }
}

// --- Stage ---

type Stage []*Actor
