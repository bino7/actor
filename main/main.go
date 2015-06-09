package main
import (
    "fmt"
    "reflect"
    "github.com/codegangsta/inject"
)

type T struct{
    name string
    handlers map[reflect.Type][]Handler
    injector inject.Injector
}
func NewT()*T{
    t:=& T{name:"haha",injector:inject.New(),handlers:make(map[reflect.Type][]Handler)}
    return t
}
type Handler interface{}

func (t *T)AddHandler(handlers ...Handler){
    mt:=reflect.TypeOf(handlers[0])
    if mt.NumIn()!=1 {
        return
    }
    _type:=mt.In(0)
    for _,h:=range handlers[1:]{
        validateHandler(h,_type)
    }
    t.handlers[_type]=handlers
}

func validateHandler(h Handler,_type reflect.Type)bool{
    mt:=reflect.TypeOf(h)
    if mt.NumIn()!=1 {
        return false
    }
    return mt.In(0)==_type
}

func (t *T)Handle(msg interface{}){
    _type:=reflect.TypeOf(msg)
    handlers:=t.handlers[_type]
    t.injector.Map(msg)
    for _,h:=range handlers{
        t.injector.Invoke(h)
    }
}

type TT struct{
    *T
}

func NewTT()*TT{
    tt:=&TT{NewT()}
    tt.AddHandler(tt.p,tt.p,tt.p)
    return tt
}

func (t *TT)print(s string){
    fmt.Println(t.name)
    fmt.Println(s)
}

func (t *TT)p(tt *TT){
    fmt.Println(t.name)
    fmt.Println(tt.name)
}
func main(){
    s:=SS{&S{"SOS"}}
    s.G()
}

type S struct{
    name string
}

func (s *S)G(){
    fmt.Println(reflect.TypeOf(s))
}

type SS struct{
    *S
}



