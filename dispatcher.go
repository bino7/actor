package actor

import (
	"bytes"
	"encoding/gob"
	"net"
	"reflect"
)

type Dispatcher struct {
	*BaseActor
	isLeader bool
	imports  chan *RemoteMessage
	exports  chan *RemoteMessage
	clients  map[string]net.Conn
	remotes  map[int]net.Conn
}

func newDispatcher(system *System, context *Context, dispatcherSeqPath string) *Dispatcher {
	di := dispatcherIndex(system.laddr, system.conf.dispatcher_size)
	children, _, _ := system.ZKConn().Children(dispatchers_path + "/" + di)
	isLeader := children[0] == dispatcherSeqPath

	d := &Dispatcher{
		NewBaseActor(system, context, system, dispatcher_path),
		isLeader,
		nil,
	}

	if isLeader {

		listener, err := net.Listen("tcp", ":"+(Context().conf.dispatcher_port+di))
		if err != nil {
			return err
		}

		go d.listen(listener)

	} else {
		lastDispatcherSeqPath := ""
		for i, child := range children {
			if child == dispatcherSeqPath {
				lastDispatcherSeqPath = children[i-1]
			}
		}
		go d.watchToBecomeLeader(lastDispatcherSeqPath)

		leader := children[0]
		data, _, _ := d.System().ZKConn().Get(leader)
		info := dispatcherInfoFromData(data)
		addr := info.Addr
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		d.remotes[dispatcherIndex(addr, system.conf.dispatcher_size)] = conn
	}

	return d
}
func (d *Dispatcher) dispatchImports() {
	size := d.System().conf.dispatcher_size
	lindex := dispatcherIndex(d.System().laddr, size)
	for {
		msg := <-d.imports
		if dispatcherIndex(msg.Addr, size) == lindex {
			d.Context().Tell(msg.To, msg)
		} else {
			client := d.clients[msg.Addr]
			if client != nil {
				if _, err := client.Write(remoteMessageToData(msg)); err != nil {
					panic(err)
				}
			}
		}

	}
}
func (d *Dispatcher) dispatchExports() {
	for {
		msg := <-d.exports
		_, err := d.remotes[dispatcherIndex(msg.Addr, d.System().conf.dispatcher_size)].Write(remoteMessageToData(msg))
		if err != nil {
			panic(err)
		}
	}
}
func (d *Dispatcher) receive(conn net.Conn, dispatcher chan *RemoteMessage) {
	for {
		dec := gob.NewDecoder(conn)
		msg := new(RemoteMessage)
		dec.Decode(msg)
		dispatcher <- msg
	}
}
func (d *Dispatcher) watchToBecomeLeader(lastDispatcherPath string) {
	existed, _, event, _ := d.System().ZKConn().ExistsW(lastDispatcherPath)
	if existed == false {
		d.becomeLeader()
		return
	}
	<-event
	d.becomeLeader()
}
func (d *Dispatcher) becomeLeader() {

}
func (d *Dispatcher) listen(listener net.Listener) {
	conn, _ := listener.Accept()
	go d.receive(conn, d.exports)
}

type Message struct {
}
type RemoteMessage struct {
	From    string
	To      string
	Addr    string
	Content []byte
}

func remoteMessageToData(msg *RemoteMessage) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(msg)
	return buf.Bytes()
}

func (d *Dispatcher) Init(props map[string]interface{}) error {
	return nil
}
func (d *Dispatcher) PreStart() error {
	/* connect to other dispatcher leader */
	lindex := dispatcherIndex(d.System(), d.System().conf.dispatcher_size)
	for i := 0; i < d.System().conf.dispatcher_size; i++ {
		if i == lindex {
			continue
		}
		path := dispatchers_path + "/" + i
		children, _, _ := d.System().ZKConn().Children(path)
		data, _, _ := d.System().ZKConn().Get(children[0])
		info := dispatcherInfoFromData(data)
		conn, _ := net.Dial("tcp", info.Addr)
		d.remotes[i] = conn
		go d.receive(conn, d.imports)
	}

	return nil
}

var remoteMessageType = reflect.TypeOf((*RemoteMessage)(nil))

func (d *Dispatcher) Receive(msg interface{}) {
	if reflect.TypeOf(msg) != remoteMessageType {
		panic("only accept " + remoteMessageType)
	}
	m := (*RemoteMessage)(msg)
	d.exports <- m
}

func (d *Dispatcher) PreStop() error {
	return nil
}
