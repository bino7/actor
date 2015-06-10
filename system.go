package actor
import (
    "sync"
    "time"
)

type System struct{
    *Context
    *Actor
}

func NewSystem(conf Config)*System{
    s:=&System{
        Actor:new(Actor),
    }
    s.Actor.Name="root"
    c:=& Context{
        system: s,
        conf:   conf,
        actor:  s.Actor,
        actors: make(map[string]*Actor),
        mutex:  sync.Mutex{},
    }
    s.Context=c
    return s
}

func ServerForever(system *System)error{
    go run(system.Actor)
    for{
        time.Sleep(10*time.Second)
    }
}