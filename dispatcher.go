package actor
import (
    "net"
    "bufio"
)

type Dispatcher struct{
    *ActorBase
    listener        net.Listener
    attachs         map[string]net.TCPConn
    dispatchers     map[string]net.TCPConn
    seqs            map[string]int64

}

func newDispatcher(system *System,context *Context) *Dispatcher {
    return & Dispatcher {
        NewActorBase(system,context,nil,""),
        nil,
        make(map[string]net.TCPConn),
        make(map[string]net.TCPConn),
        make(map[string]int64),
    }
}
func (d *Dispatcher)Init(props map[string]interface{}) error {
    d.System().ZKConn()

    listener,err:=net.Listen("tcp",":"+Context().conf.port)
    if err!=nil {
        return err
    }
    d.listener=listener
    return nil
}
func (d *Dispatcher)PreStart() error {
    go d.listen()
}
func (d *Dispatcher)Receive(msg interface{}) {

}
func (d *Dispatcher)PreStop() error {
    return nil
}
func (d *Dispatcher)listen() {
    conn, _ := d.listener.Accept()
    go d.handleConn(conn)
}

func (d *Dispatcher)handleConn(conn net.Conn) {
    reader := bufio.NewReader(conn)
    msg,_,err:=reader.ReadLine()
    if err!=nil {
        panic(err)
    }

}

type DispatcherClient struct{
    *Actor
    dispatcher  net.TCPConn
}