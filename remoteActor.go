package actor

import (
	"reflect"
)

type RemoteActor struct {
	*BaseActor
	path string
}

func newRemoteActor(system *System, context *Context, path string) *RemoteActor {
	return &RemoteActor{
		NewBaseActor(system, context, system.dispatcher, path),
		path,
	}

}

func (r *RemoteActor) Init(props map[string]interface{}) error { return nil }
func (r *RemoteActor) Start() error {
	return nil
}
func (r *RemoteActor) Receive(msg interface{}) {
	if reflect.TypeOf(msg) != reflect.TypeOf(RemoteMessage{}) {
		panic("RemoteActor only accept RemoteMessage")
	}
	r.System().dispatcher.Tell(msg)
}
func (r *RemoteActor) Stop() error {
	return nil
}
