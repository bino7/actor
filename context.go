package actor
import (
    "sync"
    "reflect"
    "encoding/gob"
    "bytes"
)

type ActorNoFoundError struct{
    path string
}

func (e ActorNoFoundError)Error()string{
    return "actor "+e.path+" not found"
}

type Context struct{
    system              *System
    conf                Config
    actors              map[string] Actor
    mutex               sync.Mutex
    dispatcher          *Dispatcher
    dispatcherClient    *DispatcherClient
}

func (c *Context)Tell(path string,message interface{})error{
    a:=c.actors[path]
    if a==nil{
        return ActorNoFoundError{
            path,
        }
    }
    a.mailbox() <- message
    return nil
}

func (c *Context)startActor(a Actor){
    a.start()
    err:=a.PreStart()
    if err!=nil {
        panic(err)
    }
    for a.running() {
        msg:= <- a.mailbox()
        a.Receive(msg)
    }
}

func (c *Context)stopActor(a Actor){
    a.stop()
}

func (c *Context)ActorOf(path string,_type reflect.Type,props map[string]interface{})(*Actor,error){
    a:=c.actors[path]
    if a!=nil {
        return a,nil
    }

    zkConn:=c.system.ZKConn()
    existed,_,err:=zkConn.Exists(path)
    if err!=nil {
        panic(err)
    }
    if existed {
        if info,err:=c.readActorInfo(path);err!=nil{
            return nil,err
        }else {
            a=newRemoteActor(c.system, c, info.Addr)
            return a, nil
        }
    }

    //create new actor
    created:=createDirectory(c.system,path)
    if created {
        newObjPtr := reflect.New(_type.Elem()).Interface()
        a=newObjPtr.(*Actor)
        if err:=a.Init(props);err!=nil {
            return nil,err
        }
        c.actors[path]=a
        c.startActor(a)
        return a,nil
    }else{
        return nil,nil
    }
}

func (c *Context)ActorSelection(path string)*Actor{
    a:=c.actors[path]
    if a==nil && isExist(c.system,path){
        a=newRemoteActor(c.system,c,path)
    }
    return a
}

func (c *Context)readActorInfo(path string)(*ActorInfo,error){
    zkConn:=c.system.ZKConn()
    if b,_,err:=zkConn.Get(path);err!=nil{
        return nil,err
    }else{
        buf:=bytes.NewBuffer(b)
        dec:=gob.NewDecoder(buf)
        var ai ActorInfo
        dec.Decode(&ai)
        return ai,nil
    }
}

type ActorInfo struct{
    Addr    string
}

func (c *Context)newDispatcher(){
    
}

func (c *Context)newDispatcherClient(leader string){

}

func (c *Context)onDispatcherChanged(leader string){
    if c.system.conf.displayName!=leader{
        if c.dispatcher!=nil{
            c.dispatcher.Stop()
        }

        if c.dispatcherClient==nil{
            c.newDispatcherClient(leader)
        }else{
            c.dispatcherClient.onDispatcherChanged(leader)
        }
    }else{
        if c.dispatcher==nil{
            c.newDispatcher()
        }else{
            c.dispatcher.onDispatcherChanged(leader)
        }
    }
}

