package actor
import (
    "fmt"
    "math/rand"
    "log"
    "time"
    "reflect"
    "testing"
)
/*run Test_PingPong_1 and Test_PingPong_2
   Test_PingPong_1 create two actors p1 and p2,
   Test_PingPong_2 create tow actors p3 and p4,
   and you can see two ball are passed between four players.
*/
func Test_PingPong_1(t *testing.T){
    log.Println("start")
    conf:=Config{
        []string{"127.0.0.1"},
        []string{"127.0.0.1"},
        "bino",
        9000,
        1,
    }
    sys:=NewSystem(conf)
    out:=sys.Start()
    p1,err:=startPlayer(sys,"p1")
    if err!=nil{
        panic(err)
    }
    fmt.Println(p1.Path())
    p2,err:=startPlayer(sys,"p2")
    if err!=nil{
        panic(err)
    }
    fmt.Println(p2.Path())

    sys.Context().RegisterDecoder(reflect.TypeOf(&Ball{}).String(),BallDecoder)
    sys.Context().RegisterEncoder(reflect.TypeOf(&Ball{}).String(),BallEncoder)
    time.Sleep(3*time.Second)
    p2.Tell(&Ball{
        "",
    })

    for{
        o:=<-out
        fmt.Println(o)
    }
}

func Test_PingPong_2(t *testing.T){
    log.Println("start")
    conf:=Config{
        []string{"127.0.0.1"},
        []string{"127.0.0.1"},
        "bino",
        9000,
        1,
    }
    sys:=NewSystem(conf)
    out:=sys.Start()
    p1,err:=startPlayer(sys,"p3")
    if err!=nil{
        panic(err)
    }
    fmt.Println(p1.Path())
    p2,err:=startPlayer(sys,"p4")
    if err!=nil{
        panic(err)
    }
    fmt.Println(p2.Path())

    sys.Context().RegisterDecoder(reflect.TypeOf(&Ball{}).String(),BallDecoder)
    sys.Context().RegisterEncoder(reflect.TypeOf(&Ball{}).String(),BallEncoder)
    time.Sleep(3*time.Second)
    p2.Tell(&Ball{
        "",
    })

    for{
        o:=<-out
        fmt.Println(o)
    }
}

func startPlayer(sys *System,name string)(Actor,error){
    path:="/actor/system/players/"+name
    p:=& Player{
        NewBaseActor(sys,sys.Context(),sys,path),
        name,
        nil,
    }
    ok,err:=sys.Context().ActorOf(path,p)
    if ok {
        return p,err
    }else{
        return nil,err
    }
}

type Ball struct{
    From    string
}

func BallDecoder(data []byte)(interface{},error){
    ball:=new(Ball)
    return GobDecode(data,ball)
}

func BallEncoder(value interface{})([]byte,error){
    return GobEncode(value)
}



type Player struct {
    *BaseActor
    name    string
    others  []Actor
}


func (p *Player)Init(props map[string]interface{}) error{
    p.name=props["name"].(string)
    return nil
}
func (p *Player)PreStart() error{
    return nil
}
func (p *Player)Receive(msg interface{}){

    ball:=msg.(*Ball)

    log.Println(p.name,"receive ball from",ball.From)

    if p.others==nil{
        players, _, err := p.System().ZKConn().Children("/actor/system/players")
        if err!=nil {
            panic(err)
        }
        p.others=make([]Actor,len(players)-1)
        i:=0
        for _,name:=range players {
            if name==p.name{
                continue
            }
            p.others[i]=p.Context().ActorSelection("/actor/system/players/"+name)
            i++
        }
    }

    from:=ball.From
    if from!="" {
        found:=false
        for _,o:=range p.others {
            if o.Path()==from{
                found=true
                break
            }
        }
        if from == p.Path(){
            found=true
        }
        if found==false {
            na:=p.Context().ActorSelection(from)
            p.others=append(p.others,na)
        }
    }

    ball.From=p.Path()
    i:=rand.Intn(len(p.others))
    na:=p.others[i]
    time.Sleep(500*time.Millisecond)
    na.Tell(ball)
}
func (p *Player)PreStop() error{
    return nil
}