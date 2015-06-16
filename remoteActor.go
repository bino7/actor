package actor
import (
    "net"
    "bufio"
    "encoding/gob"
    "reflect"
)


type RemoteActor struct {
    *ActorBase
    conn net.TCPConn
    enc *gob.Encoder
}

func newRemoteActor(system *System,context *Context,addr string) *RemoteActor {
    conn,err:=net.Dial("tcp",addr)
    if err!=nil {
        panic(err)
    }

    writer:=bufio.NewWriter(conn)
    enc:=gob.NewEncoder(writer)

    return & RemoteActor{
        NewActorBase(system,context,nil,"remote actor"),
        conn,
        enc,
    }

}

func (r *RemoteActor)Init(props map[string]interface{}) error{return nil}
func (r *RemoteActor)Start() error{
    go r.readLoop()
    return nil
}
func (r *RemoteActor)Receive(msg interface{}){
    if reflect.TypeOf(msg)!=reflect.TypeOf(RemoteMessage{}) {
        panic("RemoteActor only accept RemoteMessage")
    }
    r.enc.Encode(msg)
}
func (r *RemoteActor)Stop()  error{
    r.conn.Close()
    return nil
}
func (r *RemoteActor)readLoop() {
    reader := bufio.NewReader(r.conn)
    dec:=gob.NewDecoder(reader)
    for{
        var msg RemoteMessage
        dec.Decode(msg)
    }
}

type RemoteMessage struct{
    From    string
    To      string
    Addr    string
    Content []byte
}

