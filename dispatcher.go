package actor

import (
	"encoding/gob"
	"net"
	"reflect"
	"strconv"
	"fmt"
	"log"
)

type Dispatcher struct {
	*BaseActor
	isLeader bool
	imports  chan *RemoteMessage
	exports  chan *RemoteMessage
	clients  map[string]net.Conn
	remotes  map[int]net.Conn
}

func newDispatcher(system *System, context *Context, isLeader bool, leaderAddr string) (*Dispatcher,error) {
	fmt.Println(isLeader)
	di := dispatcherIndex(system.laddr, system.conf.DispatcherSize)

	d := & Dispatcher{
		NewBaseActor(system, context, system, dispatcher_path),
		isLeader,
		make(chan *RemoteMessage),
		make(chan *RemoteMessage),
		make(map[string]net.Conn),
		make(map[int]net.Conn),
	}

	if isLeader {

		listener, err := net.Listen("tcp", ":"+strconv.Itoa(system.conf.DispatcherPort+di))
		if err != nil {
			return nil,err
		}

		go d.listen(listener)

	} else {
		conn, err := net.Dial("tcp", leaderAddr+":"+strconv.Itoa(system.conf.DispatcherPort))
		if err != nil {
			panic(err)
		}
		d.remotes[dispatcherIndex(leaderAddr, system.conf.DispatcherSize)] = conn
		msg:=& ConnectMessage{
			d.System().displayName,
		}
		data,_:=ConnectMessageEncode(msg)
		conn.Write(data)
	}

	return d,nil
}
func (d *Dispatcher) dispatchImports() {
	lsysname:=d.System().displayName
	for {
		msg := <-d.imports
		log.Println("dispatch import msg",msg)
		if msg.SysName == lsysname {
			value,_:=d.Context().Decode(msg.Type,msg.Data)
			d.Context().Tell(msg.To, value)
		} else {
			client := d.clients[msg.SysName]
			if client != nil {
				data,_:=RemoteMessageEncode(msg)
				if _, err := client.Write(data); err != nil {
					panic(err)
				}
			}
		}

	}
}
func (d *Dispatcher) dispatchExports() {
	for {
		msg := <-d.exports
		data,_:=RemoteMessageEncode(msg)
		_, err := d.remotes[dispatcherIndex(msg.Addr, d.System().conf.DispatcherSize)].Write(data)
		log.Println("send msg",msg)
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
		log.Println("receive msg",msg)
		dispatcher <- msg
	}
}

func (d *Dispatcher) becomeLeader() {

}
func (d *Dispatcher) listen(listener net.Listener) {
	conn, _ := listener.Accept()
	dec := gob.NewDecoder(conn)
	msg := new(ConnectMessage)
	dec.Decode(msg)
	d.clients[msg.From]=conn
	log.Println("new connection from",msg.From,conn.RemoteAddr().String())
	go d.receive(conn, d.exports)
}
type ConnectMessage struct {
	From 	string
}
func ConnectMessageEncode(value *ConnectMessage)([]byte,error){
	return GobEncode(value)
}
type RemoteMessage struct {
	From    string
	To      string
	SysName string
	Addr    string
	Type 	string
	Data	[]byte
}
func RemoteMessageEncode(value *RemoteMessage)([]byte,error){
	return GobEncode(value)
}
func RemoteMessageDecode(data []byte)(interface{},error){
	info:=new(RemoteMessage)
	return GobDecode(data,info)
}

func (d *Dispatcher) Init(props map[string]interface{}) error {
	return nil
}
func (d *Dispatcher) PreStart() error {
	/* connect to other dispatcher leader */
	lindex := dispatcherIndex(d.System().laddr, d.System().conf.DispatcherSize)
	for i := 0; i < d.System().conf.DispatcherSize; i++ {
		if i == lindex {
			continue
		}
		path := dispatchers_path + "/" + strconv.Itoa(i)
		children, _, _ := d.System().Children(path)
		data, _, _ := d.System().Get(children[0])
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
		panic("only accept " + remoteMessageType.String())
	}
	m := msg.(*RemoteMessage)
	log.Println("dispatcher receive msg",m)
	d.exports <- m
}

func (d *Dispatcher) PreStop() error {
	return nil
}
