package actor
import (
    "reflect"
    "github.com/codegangsta/inject"
    "sync"
)

type Handler interface{}

type Actor struct{
    Name string
    mailbox chan interface{}
    parent *Actor
    children []*Actor
    System *System
    Context *Context
    handlers map[reflect.Type]Handler
    injector inject.Injector
    mutex sync.Mutex
}

func newActor(system *System,context *Context,parent *Actor,name string)*Actor{
    mailbox:=make(chan interface{},256)
    children:=[]*Actor
    handlers:=make(map[string]Handler)
    a:=& Actor{System:system, Context:context,parent:parent,Name:name,mailbox:mailbox,children:children,
        handlers:handlers,injector:inject.New()}
    return a
}


func (a *Actor)AddHandler(_type reflect.Type,handlers ...Handler){
    for _,h:=range handlers{
        validateHandler(h,_type)
    }
    a.handlers[_type]=handlers
}

func validateHandler(h Handler,_type reflect.Type)bool{
    mt:=reflect.TypeOf(h)
    if mt.NumIn()!=1 {
        return false
    }
    return mt.In(0)==_type
}

func (a *Actor)Handle(msg interface{}){
    _type:=reflect.TypeOf(msg)
    handlers:=a.handlers[_type]
    a.injector.Map(msg)
    for _,h:=range handlers{
        a.injector.Invoke(h)
    }
}

func (a *Actor)Tell(message interface{}){
    a.Context.tell(a.Path,message)
}

func (a *Actor)lookup(names... string)*Actor{
    a.mutex.Lock()
    defer a.mutex.Unlock()
    for _,c:=range a.children{
        if c.Name==names[0]{
            if len(names)==1 {
                return c
            }else{
                return c.lookup(names[1:])
            }
        }
    }
    return nil
}

func (a *Actor)add(child *Actor){
    a.mutex.Lock()
    defer a.mutex.Unlock()
    a.children=append(a.children,child)
}

func (a *Actor)remove(child *Actor){
    a.mutex.Lock()
    defer a.mutex.Unlock()
    for i,c:=range a.children{
        if c==child{
            a.children=append(a.children[:i],a.children[i+1:]...)
            break
        }
    }
}


func (a *Actor)Path()string{
    path:="/"+a.Name
    if a.parent!=nil{
        path=a.parent.Path()+path
    }
    return path
}