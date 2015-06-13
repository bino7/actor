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
    *Actor
    conf        Config
    zkConn      *zk.Conn
    role        Role
    addr        string
}

func NewSystem(conf Config)*System{
    s:=new(System)

    if conf.name==""{
        conf.name=="worker_1"
    }
    if conf.randName==""{
        conf.randName=randName()
    }

    s.conf=conf
    c:=& Context{
        system:     s,
        conf:       conf,
        actors:     make(map[string] *Actor),
        mutex:      sync.Mutex{},
    }
    a:=newActor(s,c,nil,"system")
    s.Actor=a
    c.actor=a
    c.actors["system"]=a
    go run(s.Actor)
    return s
}

func (sys *System)ServerForever()(err error){
    for{
        zookeeperServers:=sys.conf.zookeeperServers
        if zookeeperServers==nil || len(zookeeperServers)==0{
            log.Println("zookeeper server config not found standalone mode")
        }else{
            sys.connectToZK()
        }
    }
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
    sys.join()
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
    root_path       = "/actors"
    servers_path    = "/actors/servers"
    workers_path    = "/actors/workers"
    system_path     = "/actors/system"
)

func (sys *System)ensureBaseDir(){

    sys.zkConn.Create(root_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(servers_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(workers_path,[]byte{},zk.FlagPersistent,acl)
    sys.zkConn.Create(system_path,[]byte{},zk.FlagPersistent,acl)
}

func (sys *System)register(){
    name:=sys.conf.name

    existed,_,_:=sys.zkConn.Exists(workers_path+"/"+name)

    for existed{
        if randName,_,err:=sys.zkConn.Get(workers_path+"/"+name);err!=nil{
           continue
        }else if randName==sys.conf.randName{
            //registered
            return
        }else if i:=strings.LastIndex(name,"_");i!=-1 {
            name=name[0:i]+string(strconv.Atoi(name[i:len(name)])+1)
        }else{
            name=name+"_1"
        }
        existed,_,_=sys.zkConn.Exists(workers_path+"/"+name)
    }

    sys.conf.name=name

    sys.zkConn.Create(workers_path+"/"+sys.conf.name,[]byte{},zk.FlagPersistent,acl)
}

func (sys *System)join(){
    path:=workers_path+"/"+sys.conf.name+"/connecting"

    _,err:=sys.zkConn.Create(path,[]byte{},zk.FlagEphemeral,acl)
    if err!=nil{
        panic(err)
    }

    if _,err=sys.zkConn.Set(path,os.Hostname());err!=nil{
        panic(err)
    }

    path,err=sys.zkConn.Create(servers_path+"/",[]byte{},zk.FlagEphemeralSequential,acl)
    if err!=nil{
        panic(err)
    }

    if _,err=sys.zkConn.Set(path,sys.conf.name);err!=nil{
        panic(err)
    }

    go sys.watchServersPath()

}

type leader struct {
    name    string
    addr    string
}
var leader leader
func (sys *System)watchServersPath(){
    for {
        paths, _, event , _ := sys.zkConn.ChildrenW(servers_path)
        lname,_,_:=sys.zkConn.Get(paths[0])
        addr,_,_:=sys.zkConn.Get(workers_path+"/"+lname+"/connecting")
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

