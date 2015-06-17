package actor
import (
    "net"
    "bufio"
    "encoding/gob"
)

type Dispatcher struct{
    *ActorBase
    listener        net.Listener
    clients         map[string]net.TCPConn
    dispatchers     map[string]net.TCPConn
    seqs            map[string]int64
}

func newDispatcher(system *System,context *Context) *Dispatcher {
    dispatcher := & Dispatcher {
        NewActorBase(system,context,nil,"dispatcher"),
        nil,
        make(map[string]net.TCPConn),
        make(map[string]net.TCPConn),
        make(map[string]int64),
    }
    return dispatcher
}
func (d *Dispatcher)Init(props map[string]interface{}) error {
    listener,err:=net.Listen("tcp",":"+Context().conf.dispatcher_port)
    if err!=nil {
        return err
    }
    d.listener=listener
    return nil
}

func (d *Dispatcher)Receive(msg interface{}) {

}

func (d *Dispatcher)Start(){
    ds:=d.System().conf.dispatcher_size

    for i:=0;i<ds;i++ {
        go d.watchDispatcher(i)
    }
    d.listen()
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

func (d *Dispatcher)listen() {
    conn, _ := d.listener.Accept()
    go d.handleConn(conn)
}

func (d *Dispatcher)Stop(){

}

type HandShake struct {
    from    string
}

func (d *Dispatcher)handleConn(conn net.Conn) {
    hs:=new(HandShake)
    reader := bufio.NewReader(conn)
    dec:=gob.NewDecoder(reader)
    dec.Decode(hs)

}

func (d *Dispatcher)onDispatcherChanged(leader string){

}

type DispatcherClient struct{
    *ActorBase
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

    return & RemoteActor{
        NewActorBase(system,context,nil,"dispatcher"),
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