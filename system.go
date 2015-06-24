package actor

import (
	"bytes"
	"encoding/gob"
	"github.com/samuel/go-zookeeper/zk"
	"hash/fnv"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Role int

const (
	Leader_Role Role = 1 << iota
	Dispatcher_Role
)

var acl = []zk.ACL{
	zk.ACL{
		zk.PermAll,
		"world",
		"anyone",
	},
}

type System struct {
	*BaseActor
	displayName string
	conf        Config
	zkConn      *zk.Conn
	role        Role
	laddr       string
	dispatcher  *Dispatcher
	mutex 		sync.Mutex
}

func NewSystem(conf Config) *System {
	s := &System{}
	s.mutex=sync.Mutex{}
	if conf.Name == "" {
		conf.Name = randName()
	}

	s.conf = conf

	c := &Context{
		system: 	s,
		conf:   	conf,
		actors: 	make(map[string]Actor),
		encoders:	make(map[string]func(value interface{})([]byte,error)),
		decoders: 	make(map[string]func(data []byte)(interface{},error)),
		mutex:  	sync.Mutex{},
	}

	s.BaseActor=NewBaseActor(s,c,nil,"/system")

	c.actors["system"] = s

	if ifi, err := net.InterfaceByName("wlan0"); err != nil {
		panic(err)
	} else {
		addrs, _ := ifi.Addrs()
		s.laddr = strings.Split(addrs[0].String(), "/")[0]
	}

	return s
}

func (s *System) Init(props map[string]interface{}) error {


	return nil
}

func (s *System) PreStart() error {
	return nil
}

func (s *System) Receive(msg interface{}) {

}

func (s *System) PreStop() error {
	return nil
}

func (s *System) Start() chan interface{}{

	zookeeperServers := s.conf.ZookeeperServers
	if zookeeperServers == nil || len(zookeeperServers) == 0 {
		log.Println("zookeeper server config not found")
	} else {
		s.connectToZK()
	}

	s.Context().startActor(s)
	out:=make(chan interface{})
	return out
}

func (s *System) connectToZK() {

	zookeeperServers := s.conf.ZookeeperServers
	var err error
	var event <-chan zk.Event
	s.zkConn,event,err = zk.Connect(zookeeperServers, 2*time.Second)
	if err != nil {
		panic(err)
	}
	go s.watch(event)
	s.ensureBaseDir()
	s.register()
}

func (s *System)watch(event <-chan zk.Event){
	for e:= range event {
		log.Println(e)
	}
	log.Println("quit watch ")
}

/*
   p:PERSISTENT,ps:PERSISTENT_SEQUENTIAL,e:EPHEMERAL,es:EPHEMERAL_SEQUENTIAL,c:CONTAINER

   /actors(p)
   /actors/leader(p)
   /actors/systems(p)/{name_1}(p,name of actor_server)
                     /{name_2}(p,name of actor_server)
          /actor_1(p,node of actor_1)/actor_1_1(p,node of actor_1_1)
          /actor_2(p,node of actor_2)
*/
const (
	root_path        = "/actor"
	logins_path      = root_path + "/logins"
	systems_path     = root_path + "/systems"
	system_path      = root_path + "/system"
	dispatchers_path = system_path + "/dispatchers"
	process_path     = system_path + "/process"

	dispatcher_path = "/system/dispatcher"
)

func (sys *System) ensureBaseDir() {

	createDirectory(sys, root_path)
	createDirectory(sys, logins_path)
	createDirectory(sys, systems_path)
	createDirectory(sys, system_path)
	createDirectory(sys, dispatchers_path)
	createDirectory(sys, process_path)
}

func (s *System) register() {
	name := s.conf.Name
	displayName := name

	existed, _, _ := s.zkConn.Exists(systems_path + "/" + displayName)

	for existed {
		if i := strings.LastIndex(displayName, "_"); i != -1 {
			n,_:=strconv.Atoi(displayName[i:len(displayName)])
			n++
			displayName = displayName[0:i] + string(n)
		} else {
			displayName = displayName + "_1"
		}
		existed, _, _ = s.zkConn.Exists(systems_path + "/" + displayName)
	}

	s.displayName = displayName

	path, err := s.zkConn.Create(systems_path+"/"+s.displayName, systemInfoToData(s), zk.FlagEphemeral, acl)
	if err != nil {
		panic(err)
	}

	s.zkConn.Set(path, []byte(name), int32(0))

	s.createDispatcher()
}

type SystemInfo struct {
	Name string
	Addr string
}

func systemInfoToData(sys *System) []byte {
	info := &SystemInfo{
		sys.conf.Name,
		sys.laddr,
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(info)
	return buf.Bytes()
}
func systemInfoFromBytes(data []byte) *SystemInfo {
	info := new(SystemInfo)
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(info)
	return info
}

func (s *System) createDispatcher() {

	ifi, err := net.InterfaceByName("wlan0")
	if err != nil {
		panic(err)
	}

	laddrs, _ := ifi.Addrs()
	laddr := strings.Split(laddrs[0].String(), "/")[0]
	s.laddr = laddr
	di := dispatcherIndex(laddr, s.conf.DispatcherSize)
	createDirectory(s,dispatchers_path+"/"+strconv.Itoa(di))
	leader,seqPath, event, err := sequent(s, dispatchers_path+"/"+strconv.Itoa(di)+"/")
	if err != nil {
		panic(err)
	}
	s.Set(seqPath,dispatcherInfoToData(s))
	isLeader := leader==seqPath
	log.Println("leader=",leader,"seqPath=",seqPath)
	leaderAddr := ""
	if isLeader==false {
		log.Println(leader)
		data, _, err := s.Get(leader)
		if err != nil {
			panic(err)
		}
		info := dispatcherInfoFromData(data)
		leaderAddr = info.Addr

	} else {
		go func() {
			<-event
			s.onBecomeDispatcherLeader()
		}()
	}
	log.Println(isLeader,leaderAddr)
	s.dispatcher,err = newDispatcher(s, s.Context(), isLeader, leaderAddr)
	if err!=nil {
		panic(err)
	}
	s.Context().startActor(s.dispatcher)
	s.Context().actors[dispatcher_path] = s.dispatcher
}
func (s *System) onBecomeDispatcherLeader() {
	s.Context().stopActor(s.dispatcher)
	s.Context().onDispatcherDisable()
	var err error
	s.dispatcher,err= newDispatcher(s, s.Context(), true, "")
	if err!=nil {
		panic(err)
	}

	s.Context().onDispatcherEnable()
}

type dispatcherInfo struct {
	Name string
	Addr string
}

func dispatcherInfoToData(s *System) []byte {
	info := &dispatcherInfo{
		s.displayName,
		s.laddr,
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(info)
	return buf.Bytes()
}
func dispatcherInfoFromData(data []byte) *dispatcherInfo {
	buf := bytes.NewBuffer(data)
	info := new(dispatcherInfo)
	dec := gob.NewDecoder(buf)
	dec.Decode(info)
	return info
}

func dispatcherIndex(addr string, dispatcherSize int) int {
	h := fnv.New64()
	h.Write([]byte(addr))
	return int(h.Sum64() % uint64(dispatcherSize))
}

type leader struct {
	name string
	addr string
}

func (sys *System) onLeaderChanged() {

}

var chars = []string{"a", "b", "c", "d", "e", "f", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
	"A", "B", "C", "D", "E", "F", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
	"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

func randName() (name string) {
	cs := len(chars)
	for i := 0; i < 16; i++ {
		name += chars[rand.Intn(cs)]
	}
	return
}

func (s *System) ZKConn() *zk.Conn {
	return s.zkConn
	/*for {
		zookeeperServers := s.conf.ZookeeperServers
		d, _ := time.ParseDuration("24h")
		var err error
		conn, _, err := zk.Connect(zookeeperServers, d)
		if err != nil {
			//log.Println(err)
			continue
		}
		s.zkConn=conn
		return s.zkConn
	}*/
}

func (s *System) Set(path string,data []byte){
	s.ZKConn().Set(path,data,int32(0))
}

func (s *System) Get(path string)([]byte, *zk.Stat, error){
	return s.ZKConn().Get(path)
}

func (s *System) Children(path string)([]string, *zk.Stat, error){
	return s.ZKConn().Children(path)
}

func (s *System) setRole(set Role) {
	s.role = s.role | set
}

func (s *System) unsetRole(unset Role) {
	s.role = s.role ^ unset
}

func (s *System) isRole(role Role) bool {
	return s.role&role > 0
}
