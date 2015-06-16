package actor
import (
    "reflect"
    "github.com/codegangsta/inject"
    "sync"
    "cmd/api/testdata/src/pkg/p1"
)

type Actor interface{
    Init(props map[string]interface{}) error
    PreStart() error
    Receive(msg interface{})
    PreStop()  error

    /*implement by ActorBase*/
    Name() string
    Path() string
    Parent() *Actor
    System() *System
    Context() *Context
    Tell(msg interface{})
    mailbox() <-chan interface{}
    handle(msg interface{})
    start()
    stop()
    running() bool
}
type Handler interface{}

/* ActorBase is not a actor, it is base behavior of a actor*/
type ActorBase struct{
    name            string
    path            string
    mailbox         chan interface{}
    parent          Actor
    children        []Actor
    system          *System
    context         *Context
    handlers        map[reflect.Type][]Handler
    injector        inject.Injector
    mutex           *sync.Mutex
    running         bool
}

func NewActorBase(system *System,context *Context,parent *Actor,name string) *ActorBase {
    return & ActorBase{
        name:       name,
        path:       "",
        mailbox:    make(chan interface{},256),
        parent:     parent,
        children:   make([]Actor,0),
        system:     system,
        context:    context,
        handlers:   make(map[reflect.Type][]Handler),
        injector:   inject.New(),
        mutex:      &sync.Mutex{},
    }
}

func (a *ActorBase)Name() string {
    return a.name
}
func (a *ActorBase)Path()string{
    return a.path
}
func (a *ActorBase)Parent() *Actor {
    return a.parent
}
func (a *ActorBase)System() *System {
    return a.system
}
func (a *ActorBase)Context() *Context {
    return a.context
}
func (a *ActorBase)Tell(msg interface{}){
    a.Context.Tell(a.Path(),msg)
}

func (a *ActorBase)AddHandler(_type reflect.Type,handlers ...Handler){
    for _,h:=range handlers{
        validateHandler(h,_type)
    }
    a.handlers[_type]=handlers
}

func validateHandler(h Handler,_type reflect.Type)bool{
    if reflect.TypeOf(h).Kind() != reflect.Func{
        return false
    }
    mt:=reflect.TypeOf(h)
    if mt.NumIn()!=1 {
        return false
    }
    return mt.In(0)==_type
}

func (a *ActorBase)handle(msg interface{}){
    _type:=reflect.TypeOf(msg)
    handlers:=a.handlers[_type]
    a.injector.Map(msg)
    for _,h:=range handlers{
        a.injector.Invoke(h)
    }
}

func (a *ActorBase)add(child *Actor){
    a.mutex.Lock()
    defer a.mutex.Unlock()
    a.children=append(a.children,child)
}

func (a *ActorBase)remove(child *Actor){
    a.mutex.Lock()
    defer a.mutex.Unlock()
    for i,c:=range a.children{
        if c==child{
            a.children=append(a.children[:i],a.children[i+1:]...)
            break
        }
    }
}

func (a *ActorBase)ZooPath()string{
    return system_path+a.Path()[len("/system"):len(a.Path())]
}

func (a *ActorBase)start(){
    a.running=true
}
func (a *ActorBase)running()bool{
    return a.running
}
func (a *ActorBase)stop(){
    a.running=false
}




