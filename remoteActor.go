package actor
import (
    "reflect"
)


type RemoteActor struct {
    *ActorBase
    path string
}

func newRemoteActor(system *System,context *Context,path string) *RemoteActor {
    return & RemoteActor{
        NewActorBase(system,context,nil,path),
        path,
    }

}

func (r *RemoteActor)Init(props map[string]interface{}) error{return nil}
func (r *RemoteActor)Start() error{
    return nil
}
func (r *RemoteActor)Receive(msg interface{}){
    if reflect.TypeOf(msg)!=reflect.TypeOf(RemoteMessage{}) {
        panic("RemoteActor only accept RemoteMessage")
    }
}
func (r *RemoteActor)Stop()  error{
    return nil
}
type RemoteMessage struct{
    From    string
    To      string
    Addr    string
    Content []byte
}

