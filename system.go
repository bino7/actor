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
)

type Role int

const(
    Leader Role = 1 << iota
    Worker
    Dispatcher
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
    conf        Config
    zkConn      *zk.Conn
    role        Role
    addr        string
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
    run(s)
}

func (sys *System)connectToZK(){

    zookeeperServers:=sys.conf.zookeeperServers
    d,_:=time.ParseDuration(sys.conf.tickTime)
    var err error
    sys.zkConn,_,err=zk.Connect(zookeeperServers,d)
    if err!=nil{
        panic(error)
    }

    sys.ensureBaseDir()
    sys.register()
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
    root_path           = "/actors"
    logins_path         = root_path + "/logins"
    systems_path        = root_path + "/systems"
    system_path         = root_path + "/system"
    dispatchers_path    = system_path + "/dispatchers"
    process_path        = system_path + "/process"
)

func (sys *System)ensureBaseDir(){

    sys.zkConn.Create(root_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(logins_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(systems_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(system_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(process_path,[]byte{},zk.FlagPersistent,acl)
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

