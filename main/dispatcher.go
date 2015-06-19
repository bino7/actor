package actor
import (
    "net"
    "bufio"
    "encoding/gob"
)

type DispatcherHub struct{
    *BaseActor
    listener        net.Listener
    dispatchers     map[string]*Dispatcher
    hubs            map[int]*DispatcherHub
}
func newDispatcher(system *System,context *Context,di int)*Actor{
    return nil
}

func newDispatcherHub(system *System,context *Context,di int) *DispatcherHub {
    listener,err:=net.Listen("tcp",":"+(Context().conf.dispatcher_port+di))
    if err!=nil {
        return err
    }

    return & Dispatcher {
        NewBaseActor(system,context,nil,"dispatcher_hub"),
        listener,
        make(map[string]*Dispatcher),
        make(map[int]*DispatcherHub),
    }
}
func (d *DispatcherHub)Init(props map[string]interface{}) error {
    return nil
}

func (d *DispatcherHub)Receive(msg interface{}) {

}

func (d *Dispatcher)watchDispatcher(n int){
    zkConn:=d.System().ZKConn()
    clients,event,err:=zkConn.ChildrenW(dispatchers_path+"/"+n)
    if err!=nil {
        panic(err)
    }
    dispatcher:=clients[0]
    if d.dispatchers[dispatcher]==nil{
        
    }
}

func (d *DispatcherHub)listen() {
    conn, _ := d.listener.Accept()
    go d.handleConn(conn)
}

func (d *Dispatcher)Stop(){

}

type HandShake struct {
    from    string
}

func (d *DispatcherHub)handleConn(conn net.Conn) {
    hs:=new(HandShake)
    reader := bufio.NewReader(conn)
    dec:=gob.NewDecoder(reader)
    dec.Decode(hs)


}

func (d *Dispatcher)onDispatcherChanged(leader string){

}

type Dispatcher struct{
    *BaseActor
    conn net.TCPConn
    enc *gob.Encoder
}

func newDispatcherClient(system *System,context *Context,addr string) *RemoteActor {
    conn,err:=net.Dial("tcp",addr)
    if err!=nil {
        panic(err)
    }

    writer:=bufio.NewWriter(conn)
    enc:=gob.NewEncoder(writer)
    msg:=new(RemoteMessage)
    enc.Encode(msg)


    return & RemoteActor{
        NewBaseActor(system,context,nil,"dispatcher"),
        conn,
        enc,
    }
}

func (r *DispatcherClient)Init(props map[string]interface{}) error{return nil}
func (r *DispatcherClient)Start() error{
    go r.readLoop()
    return nil
}
func (r *DispatcherClient)Receive(msg interface{}){
    r.enc.Encode(msg)
}
func (r *DispatcherClient)Stop()  error{
    r.conn.Close()
    return nil
}
func (r *DispatcherClient)readLoop() {
    reader := bufio.NewReader(r.conn)
    dec:=gob.NewDecoder(reader)
    for{
        var msg RemoteMessage
        dec.Decode(msg)
    }
}

func (d *DispatcherClient)onDispatcherChanged(leader string){

}