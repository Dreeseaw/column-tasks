package tasks

import (
//    "github.com/kelindar/column"
    comm "github.com/kelindar/column/commit"
)

// --- Stream ---

type commChan chan comm.Commit

// Stream implements column/commit.Logger 
type Stream struct {
    tasks map[string]commChan
    schema map[string]string
}

func NewStream(schema map[string]string) *Stream {
    return &Stream{
        tasks: make(map[string]commChan),
        schema: schema,
    }
}

// fulfills commit.Logger
func (s *Stream) Append(commit comm.Commit) error {
    for _, taskChan := range s.tasks {
        taskChan <- commit.Clone()
    }
    return nil
}

func (s *Stream) AddTask(id string) commChan {
    if _, exists := s.tasks[id]; !exists {
        taskChan := make(commChan, 1024)
        s.tasks[id] = taskChan
        return taskChan
    }
    return nil
}
