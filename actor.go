package actor

import (
	"github.com/codegangsta/inject"
	"reflect"
	"sync"
)

type Actor interface {
	Init(props map[string]interface{}) error
	PreStart() error
	Receive(msg interface{})
	PreStop() error

	/*implement by BaseActor*/
	Path() string
	Parent() Actor /*who created this actor */
	System() *System
	Context() *Context
	events() chan interface{}
	Tell(msg interface{})error
	mailbox() chan interface{}
	handle(msg interface{})
}

type Handler interface{}

type BaseActor struct {
	path     string
	messageChan  chan interface{}
	eventChan   chan interface{}
	parent   Actor
	system   *System
	context  *Context
	handlers map[reflect.Type][]Handler
	injector inject.Injector
	mutex    *sync.Mutex
}

func NewBaseActor(system *System, context *Context, parent Actor, path string) *BaseActor {
	return &BaseActor{
		path:     path,
		messageChan:  make(chan interface{}, 10),
		eventChan:   make(chan interface{}),
		parent:   parent,
		system:   system,
		context:  context,
		handlers: make(map[reflect.Type][]Handler),
		injector: inject.New(),
		mutex:    &sync.Mutex{},
	}
}

func (a *BaseActor) Path() string {
	return a.path
}
func (a *BaseActor) Parent() Actor {
	return a.parent
}
func (a *BaseActor) System() *System {
	return a.system
}
func (a *BaseActor) Context() *Context {
	return a.context
}
func (a *BaseActor) Tell(msg interface{})error{
	return a.Context().Tell(a.Path(), msg)
}

func (a *BaseActor) AddHandler(_type reflect.Type, handlers ...Handler) {
	for _, h := range handlers {
		validateHandler(h, _type)
	}
	a.handlers[_type] = handlers
}

func validateHandler(h Handler, _type reflect.Type) bool {
	if reflect.TypeOf(h).Kind() != reflect.Func {
		return false
	}
	mt := reflect.TypeOf(h)
	if mt.NumIn() != 1 {
		return false
	}
	return mt.In(0) == _type
}

func (a *BaseActor) handle(msg interface{}) {
	_type := reflect.TypeOf(msg)
	handlers := a.handlers[_type]
	a.injector.Map(msg)
	for _, h := range handlers {
		a.injector.Invoke(h)
	}
}

func (a *BaseActor) ZooPath() string {
	return system_path + a.Path()[len("/system"):len(a.Path())]
}

func (a *BaseActor) events() chan interface{}{
	return a.eventChan
}

func (a *BaseActor) mailbox() chan interface{}{
	return a.messageChan
}