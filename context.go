package actor
import (
    "sync"
)

type ActorNoFoundError struct{
    path string
}

func (e ActorNoFoundError)Error()string{
    return "actor "+e.path+" not found"
}

type ActorExistedError struct{
    path string
}

func (e ActorExistedError)Error()string{
    return "actor "+e.path+" existed"
}



type Context struct{
    system *System
    conf Config
    actor *Actor
    actors map[string] *Actor
    mutex sync.Mutex
}
func newContext(c *Context,actor *Actor) *Context{
    return & Context{
        system: c.system,
        conf:   c.conf,
        actor:  actor,
        actors: c.actors,
        mutex:  c.mutex,
    }
}

func (c *Context)Tell(path string,message interface{})error{
    a:=c.actors[path]
    if a==nil{
        return ActorNoFoundError{
            path,
        }
    }
    a.mailbox <- message
    return nil
}

func (c *Context)ActorOf(name string)(actor *Actor,error error){
    c.mutex.Lock()
    defer c.mutex.Unlock()
    path:=c.actor.Path()+"/"+name
    if c.actors[path]!=nil{
        error=ActorExistedError{
           path,
       }
        return
    }
    actor=newActor(c.system,nil,c.actor,name)
    actor.Context=newContext(c,actor)
    c.actors[actor.Path()]=actor
    go run(actor)
    return
}

func run(a *Actor){
    for{
        msg:= <- a.mailbox
        a.Handle(msg)
    }
}
