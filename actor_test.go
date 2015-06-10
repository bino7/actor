package actor
import (
    "testing"
    "fmt"
)

func Test(t *testing.T){
    s:=NewSystem(Config{})
    client,_:=s.ActorOf("client")
    fmt.Println(client.Path())
    panic(ServerForever(s))
}