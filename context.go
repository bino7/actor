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
    actor       *Actor
    actors      map[string] *Actor
    mutex       sync.Mutex
}
func newContext(c *Context,actor *Actor) *Context{
    return & Context{
        system:     c.system,
        conf:       c.conf,
        actor:      actor,
        actors:     c.actors,
        mutex:      c.mutex,
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

func (c *Context)ActorOf(name string,_type reflect.Type,props map[string]interface{})(created bool,actor *Actor){
    c.mutex.Lock()
    defer c.mutex.Unlock()
    path:=c.actor.Path()+"/"+name
    if existed,_,_:=c.system.zkConn.Exists(c.actor.ZooPath()+"/"+name);existed{

    }

    var a *Actor
    if c.actors[path]!=nil{
        a=c.actors[path]
        return false,a
    }
    //newObjPtr := reflect.New(_type.Elem()).Interface()
    //t:=newObjPtr.(_type.Elem)

    a=newActor(c.system,nil,c.actor,name)
    a.Context=newContext(c,a)
    c.actors[actor.Path()]=a
    go run(actor)
    return true,a
}

func run(a *Actor){
    for{
        msg:= <- a.mailbox
        a.Handle(msg)
    }
}

/*
func (c *Context)Watch(path string){
    c.mutex.Lock()
    defer c.mutex.Unlock()
    watchers:=c.watchers[path]
    if watchers==nil{
        watchers=make([]*Actor,0)
    }
    for _,w:=range watchers{
        if c.actor==w{
            return
        }
    }
    c.watchers[path]=append(watchers,c.actor)
}

func (c *Context)UnWatch(path string){
    c.mutex.Lock()
    defer c.mutex.Unlock()
    watchers:=c.watchers[path]
    if watchers==nil{
        return
    }
    for i,w:=range watchers{
        if c.actor==w{
            if len(watchers)-1>i{
                c.watchers[path]=append(watchers[:i],watchers[i+1:]...)
            }else{
                c.watchers[path]=watchers[:i]
            }
        }
    }
}*/
