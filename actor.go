package actor
import (
    "reflect"
    "github.com/codegangsta/inject"
    "sync"
)

type Handler interface{}

type Actor struct{
    Name string
    path string
    mailbox chan interface{}
    parent *Actor
    children []*Actor
    System *System
    Context *Context
    handlers map[reflect.Type][]Handler
    injector inject.Injector
    mutex *sync.Mutex
}

func newActor(system *System,context *Context,parent *Actor,name string)*Actor{
    return & Actor{
        Name:       name,
        path:       "",
        mailbox:    make(chan interface{},256),
        parent:     parent,
        children:   make([]*Actor,0),
        System:     system,
        Context:    context,
        handlers:   make(map[reflect.Type][]Handler),
        injector:   inject.New(),
        mutex:      &sync.Mutex{},
    }
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
    a.Context.Tell(a.Path(),message)
}

func (a *Actor)lookup(names... string)*Actor{
    a.mutex.Lock()
    defer a.mutex.Unlock()
    for _,c:=range a.children{
        if c.Name==names[0]{
            if len(names)==1 {
                return c
            }else{
                return c.lookup(names[1:]...)
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

func (a *Actor)updatePath(){
    path:="/"+a.Name
    if a.parent!=nil{
        path=a.parent.Path()+path
    }
    a.path=path
}

func (a *Actor)Path()string{
    if a.path==""{
        a.updatePath()
    }
    return a.path
}

func (a *Actor)ZooPath()string{
    return system_path+a.Path()[len("/system"):len(a.Path())]
}



func (a *Actor)init(props map[string]interface{})error{
    return nil
}
