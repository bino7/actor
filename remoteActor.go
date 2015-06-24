package actor

import (
	"reflect"
)

type RemoteActor struct {
	*BaseActor
	path 	string
	sysname	string
	addr 	string
}

func newRemoteActor(system *System, context *Context, path string,sysname string,addr string) *RemoteActor {
	return &RemoteActor{
		NewBaseActor(system, context, system.dispatcher, path),
		path,
		sysname,
		addr,
	}

}

func (r *RemoteActor) Init(props map[string]interface{}) error { return nil }
func (r *RemoteActor) PreStart() error {
	return nil
}
func (r *RemoteActor) Receive(msg interface{}) {
	tname:=reflect.TypeOf(msg).String()
	data,err:=r.Context().Encode(tname,msg)
	if err!=nil{
		return
	}
	rmsg:=& RemoteMessage{
		From:		"",
		To:			r.path,
		SysName: 	r.sysname,
		Addr:		r.addr,
		Type:		tname,
		Data: 		data,
	}
	r.System().dispatcher.Tell(rmsg)
}
func (r *RemoteActor) PreStop() error {
	return nil
}
