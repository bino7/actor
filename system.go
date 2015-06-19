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
	*Context
	BaseActor
	displayName string
	conf        Config
	zkConn      *zk.Conn
	role        Role
	laddr       string
	dispatcher  *Dispatcher
}

func NewSystem(conf Config) *System {
	s := &System{}

	if conf.name == "" {
		conf.name = randName()
	}

	s.conf = conf

	c := &Context{
		system: s,
		conf:   conf,
		actors: make(map[string]*Actor),
		mutex:  sync.Mutex{},
	}

	c.actors["system"] = s

	if ifi, err := net.InterfaceByName("wlan0"); err != nil {
		panic(err)
	} else {
		addrs, _ := ifi.Addrs()
		s.laddr = strings.Split(addrs[0], "/")[0]
	}

	return s
}

func (s *System) Init(props map[string]interface{}) error {

	zookeeperServers := s.conf.zookeeperServers
	if zookeeperServers == nil || len(zookeeperServers) == 0 {
		log.Println("zookeeper server config not found standalone mode")
	} else {
		s.connectToZK()
	}
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

func (s *System) ServerForever() {
	s.Context.startActor(s)
}

func (s *System) connectToZK() {

	zookeeperServers := s.conf.zookeeperServers
	d, _ := time.ParseDuration(s.conf.tickTime)
	var err error
	s.zkConn, _, err = zk.Connect(zookeeperServers, d)
	if err != nil {
		panic(error)
	}
	s.ensureBaseDir()
	s.register()
	//sys.join()
}

/*
   p:PERSISTENT,ps:PERSISTENT_SEQUENTIAL,e:EPHEMERAL,es:EPHEMERAL_SEQUENTIAL,c:CONTAINER

   /actors(p)
   /actors/leader(p)
   /actors/workers(p)/{name_1}(p,name of worker)/connect(e,note worker is connecting)
                     /{name_2}(p,name of worker)/connect(e,note worker is connecting)
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

	createDirectory(sys.zkConn, root_path)
	createDirectory(sys.zkConn, logins_path)
	createDirectory(sys.zkConn, systems_path)
	createDirectory(sys.zkConn, system_path)
	createDirectory(sys.zkConn, dispatchers_path)
	createDirectory(sys.zkConn, process_path)
}

func (s *System) register() {
	name := s.conf.name
	displayName := name

	existed, _, _ := s.zkConn.Exists(systems_path + "/" + displayName)

	for existed {
		if i := strings.LastIndex(displayName, "_"); i != -1 {
			displayName = displayName[0:i] + string(strconv.Atoi(displayName[i:len(displayName)])+1)
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
		sys.conf.name,
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
	di := dispatcherIndex(laddr, s.conf.dispatcher_size)
	createDirectory(s, dispatchers_path+"/"+di)
	leaderPath, event, err := sequent(s, dispatchers_path+"/"+di+"/")
	if err != nil {
		panic(err)
	}
	isLeader := true
	leaderAddr := ""
	if event != nil {
		isLeader = false
		go func() {
			<-event
			s.onBecomeDispatcherLeader()
		}()
	} else {
		data, _, err := s.ZKConn().Get(leaderPath)
		if err != nil {
			panic(err)
		}
		info := dispatcherInfoFromData(data)
		leaderAddr = info.Addr
	}
	s.dispatcher = newDispatcher(s, s.Context(), isLeader, leaderAddr)
	s.Context().actors[dispatcher_path] = s.dispatcher
}
func (s *System) onBecomeDispatcherLeader() {
	s.Context().stopActor(s.dispatcher)
	s.Context().onDispatcherDisable()
	s.dispatcher = newDispatcher(s, s.Context(), true, "")
	s.Context().onDispatcherEnable()
}

type dispatcherInfo struct {
	Name string
	Addr string
}

func dispatcherInfoData(s *System) []byte {
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
	return h.Sum64() % dispatcherSize
}

type leader struct {
	name string
	addr string
}

var leader leader

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
}

func (s *System) setRole(set Role) {
	s.role = s.role | set
}

func (s *System) unsetRole(unset Role) {
	s.role = s.role ^ unset
}

func (s *System) isRole(role Role)bool{
	return s.role & role > 0
}
