package actor
import (
    "sync"
    "time"
    "log"
    "github.com/samuel/go-zookeeper/zk"
)

type System struct{
    *Context
    *Actor
    conf Config
    standalone bool
    zkConn *zk.Conn
    zkEvent zk.Event

}

func NewSystem(conf Config)*System{
    s:=&System{
        Actor:  new(Actor),
        conf:   conf,
    }
    s.Actor.Name="root"
    c:=& Context{
        system: s,
        conf:   conf,
        actor:  s.Actor,
        actors: make(map[string]*Actor),
        mutex:  sync.Mutex{},
    }
    s.Context=c
    go run(s.Actor)
    return s
}

func (sys *System)ServerForever()(err error){
    for{
        zookeeperServers:=sys.conf.zookeeperServers
        if zookeeperServers==nil || len(zookeeperServers)==0{
            log.Println("zookeeper server config not found standalone mode")
            sys.standalone=true
        }else{
            sys.zkConn,sys.zkEvent,err=zk.Connect(zookeeperServers,sys.conf*time.Millisecond)
            if err!=nil{
                return
            }
            sys.zkConn.C
        }
        time.Sleep(10*time.Second)
    }
}