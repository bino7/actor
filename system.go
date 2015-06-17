package actor
import (
    "sync"
    "time"
    "log"
    "github.com/samuel/go-zookeeper/zk"
    "math/rand"
    "strings"
    "strconv"
    "os"
    "net"
    "hash/fnv"
)

type Role int

const(
    Leader_Role      Role = 1 << iota
    Dispatcher_Role
)

var acl=[]zk.ACL{
        zk.ACL{
            zk.PermAll,
            "world",
            "anyone",
        },
    }

type System struct{
    *Context
    ActorBase
    conf            Config
    zkConn          *zk.Conn
    role            Role
    laddr           string
    dispatcherPath  string
}

func NewSystem(conf Config)*System{
    s:=System{}

    if conf.name==""{
        conf.name=randName()
    }
    if conf.displayName==""{
        conf.displayName=conf.name
    }

    s.conf=conf

    c:=& Context{
        system:     s,
        conf:       conf,
        actors:     make(map[string] *Actor),
        mutex:      sync.Mutex{},
    }

    c.actors["system"]=s

    if ifi,err:=net.InterfaceByName("wlan0");err!=nil {
        panic(err)
    }else{
        addrs,_:=ifi.Addrs()
        s.laddr=strings.Split(addrs[0],"/")[0]
    }

    return s
}

func (s *System)Init(props map[string]interface{}) error{

    zookeeperServers:=s.conf.zookeeperServers
    if zookeeperServers==nil || len(zookeeperServers)==0{
        log.Println("zookeeper server config not found standalone mode")
    }else{
        s.connectToZK()
    }
    return nil
}

func (s *System)PreStart() error{
    return nil
}

func (s *System)Receive(msg interface{}){

}

func (s *System)PreStop() error{
    return nil
}

func (s *System)ServerForever(){
    s.Context.startActor(s)
}

func (s *System)connectToZK(){

    zookeeperServers:=s.conf.zookeeperServers
    d,_:=time.ParseDuration(s.conf.tickTime)
    var err error
    s.zkConn,_,err=zk.Connect(zookeeperServers,d)
    if err!=nil{
        panic(error)
    }
    s.ensureBaseDir()
    s.register()
    s.registerDispatcher()

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
const(
    root_path           = "/actor"
    logins_path         = root_path + "/logins"
    systems_path        = root_path + "/systems"
    system_path         = root_path + "/system"
    dispatchers_path    = system_path + "/dispatchers"
    process_path        = system_path + "/process"
)

func (sys *System)ensureBaseDir(){

    createDirectory(sys.zkConn,root_path)
    createDirectory(sys.zkConn,logins_path)
    createDirectory(sys.zkConn,systems_path)
    createDirectory(sys.zkConn,system_path)
    createDirectory(sys.zkConn,dispatchers_path)
    createDirectory(sys.zkConn,process_path)
}

func (sys *System)register(){
    name:=sys.conf.name
    displayName:=sys.conf.displayName

    existed,_,_:=sys.zkConn.Exists(systems_path+"/"+displayName)

    for existed{
        if i:=strings.LastIndex(displayName,"_");i!=-1 {
            displayName=displayName[0:i]+string(strconv.Atoi(displayName[i:len(displayName)])+1)
        }else{
            displayName=displayName+"_1"
        }
        existed,_,_=sys.zkConn.Exists(systems_path+"/"+displayName)
    }

    sys.conf.displayName=displayName

    path,err:=sys.zkConn.Create(systems_path+"/"+sys.conf.displayName,[]byte{},zk.FlagEphemeral,acl)
    if err!=nil{
        panic(err)
    }

    sys.zkConn.Set(path,[]byte(name),int32(0))

}

func (sys *System)join(){
    path:= systems_path+"/"+sys.conf.name+"/connecting"

    _,err:=sys.zkConn.Create(path,[]byte{},zk.FlagEphemeral,acl)
    if err!=nil{
        panic(err)
    }

    if _,err=sys.zkConn.Set(path,os.Hostname());err!=nil{
        panic(err)
    }

    path,err=sys.zkConn.Create(logins_path+"/",[]byte{},zk.FlagEphemeralSequential,acl)
    if err!=nil{
        panic(err)
    }

    if _,err=sys.zkConn.Set(path,sys.conf.name);err!=nil{
        panic(err)
    }

    go sys.watchLoginsPath()

}

func (s *System)registerDispatcher(){

    if ifi,err:=net.InterfaceByName("wlan0");err!=nil{
        panic(err)
    }else {
        laddrs, _ := ifi.Addrs()
        laddr := strings.Split(laddrs[0].String(), "/")[0]
        h := fnv.New64()
        h.Write([]byte(laddr))
        dn := h.Sum64()%s.conf.dispatcher_size
        s.ZKConn().Create(dispatchers_path+"/"+dn,[]byte{},zk.FlagPersistent,acl)
        s.dispatcherPath=dispatchers_path+"/"+dn+"/"

    }
    path,_:=s.ZKConn().Create(s.dispatcherPath,[]byte{},zk.FlagEphemeralSequential,acl)
    s.ZKConn().Set(path,s.conf.displayName)
    _,_,event,_:= s.ZKConn().ChildrenW(s.dispatcherPath)
    go s.watchDispatcher(event)
}

func (s *System)watchDispatcher(event zk.Event){
    <- event
    dispatcherClients,_, event, _:=s.ZKConn().ChildrenW(s.dispatcherPath)
    leader,_,_:=s.ZKConn().Get(dispatcherClients[0])
    s.onDispatcherChanged(leader)
    go s.watchDispatcher(event)
}

type leader struct {
    name    string
    addr    string
}
var leader leader
func (sys *System)watchLoginsPath(){
    for {
        paths, _, event , _ := sys.zkConn.ChildrenW(logins_path)
        lname,_,_:=sys.zkConn.Get(paths[0])
        addr,_,_:=sys.zkConn.Get(systems_path+"/"+lname+"/connecting")
        leader=leader{
            lname,
            addr,
        }
        sys.onLeaderChanged()
        <- event
    }
}

func (sys *System)onLeaderChanged(){

}

var chars=[]string{"a","b","c","d","e","f","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
                   "A","B","C","D","E","F","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z",
                   "0","1","2","3","4","5","6","7","8","9"}
func randName()(name string){
    cs:=len(chars)
    for i:=0;i<16;i++{
        name+=chars[rand.Intn(cs)]
    }
    return
}

func (s *System)ZKConn() *zk.Conn{
    return s.zkConn
}

func (s *System)onDispatcherChanged(leader string){

    if leader==s.conf.displayName {
        s.setRole(Dispatcher)
    }

    s.Context.onDispatcherChanged(leader)
}


func (s *System)setRole(set Role){
    s.role=s.role|set
}

func (s *System)unsetRole(unset Role){
    s.role=s.role^unset
}


/*func (sys *System)watch(event <- chan zk.Event){
    for {
        e := <- event
        if e.Err!=nil {
            log.Println(e)
        }else{
            if e.State!=zk.StateDisconnected {
                watchers:=sys.watchers[e.Path]
                if watchers!=nil{
                    for _,w:=range watchers{
                        w.Tell(e)
                    }
                }
            }
        }
    }
}*/

