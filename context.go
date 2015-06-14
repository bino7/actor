package actor
import (
    "sync"
    "reflect"
)

type ActorNoFoundError struct{
    path string
}

func (e ActorNoFoundError)Error()string{
    return "actor "+e.path+" not found"
}

type Context struct{
    system      *System
    conf        Config
    actors      map[string] Actor
    mutex       sync.Mutex
}

func (c *Context)Tell(path string,message interface{})error{
    a:=c.actors[path]
    if a==nil{
        return ActorNoFoundError{
            path,
        }
    }
    a.mailbox() <- message
    return nil
}

func run(a Actor){
    for{
        msg:= <- a.mailbox()
        a.Receive(msg)
    }
}

func (c *Context)newDispatcher(){
    
}

