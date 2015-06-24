package actor

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
	"time"
)

type ActorNoFoundError struct {
	path string
}

func (e ActorNoFoundError) Error() string {
	return "actor " + e.path + " not found"
}

type Context struct {
	system 		*System
	conf   		Config
	actors 		map[string]Actor
	encoders 	map[string]func(value interface{})([]byte,error)
	decoders	map[string]func(data []byte)(interface{},error)
	mutex  		sync.Mutex
}

func (c *Context) Tell(path string, message interface{}) error {
	a := c.actors[path]
	if a == nil {
		return ActorNoFoundError{
			path,
		}
	}
	a.mailbox() <- message
	return nil
}

func (c *Context) startActor(a Actor) error {
	if err := a.PreStart(); err != nil {
		return err
	}
	go func(){
		for {
			select {
			case msg := <-a.mailbox():
				a.Receive(msg)
			case e := <-a.events():
				switch e {
				case StopEvent:
					return
				case DispatcherDisableEvent:
					/* wait for dispatcer enable */
					for {
						time.Sleep(1 * time.Second)
						e = <-a.events()
						switch e {
						case DispatcherEnableEvent:
							break
						case StopEvent:
							return
						}

					}
				}
			}

		}
	}()
	return nil
}

func (c *Context) stopActor(a Actor) error {
	a.events() <- StopEvent
	return a.PreStop()
}

func (c *Context) ActorOf(path string, actor Actor) (bool, error) {
	a:=c.ActorSelection(path)
	if a!=nil{
		return false,ActorExistedError
	}
	/*create new actor*/
	created := createEphemeralDirectory(c.system, path)
	if created == false {
		return false, nil
	}
	info:=& ActorInfo{
		c.system.displayName,
		c.system.laddr,
	}
	data,_:=ActorInfoEncode(info)
	c.system.Set(path,data)
	c.actors[path]=actor
	c.startActor(actor)
	return true,nil
	/*if a := c.actors[path]; a != nil {
		return a, nil
	}

	zkConn := c.system.ZKConn()
	existed, _, err := zkConn.Exists(path)
	if err != nil {
		panic(err)
	}
	if existed {
		if info, err := c.readActorInfo(path); err != nil {
			return nil, err
		} else {
			a := newRemoteActor(c.system, c, info.Addr)
			return a, nil
		}
	}

	//create new actor
	created := createDirectory(c.system, path)
	if created == false {
		return nil, ActorExistedError
	}

	newObjPtr := reflect.New(ty.Elem()).Interface()
	ba:=newObjPtr.(BaseActor)
	ba.system=c.system
	ba.context=c
	ba.parent=nil
	ba.path=path
	a := ba.(Actor)
	err = a.Init(props)
	return a, err*/
}

func (c *Context) ActorSelection(path string) Actor {
	a := c.actors[path]
	if a == nil && isExist(c.system, path) {
		data,_,_:=c.system.Get(path)
		value,_:=ActorInfoDecode(data)
		info:=value.(*ActorInfo)
		sysname:=info.SysName
		addr:=info.Addr
		a = newRemoteActor(c.system, c, path,sysname,addr)
		c.actors[path]=a
		c.startActor(a)
	}
	return a
}

func (c *Context) readActorInfo(path string) (*ActorInfo, error) {
	if b, _, err := c.system.Get(path); err != nil {
		return nil, err
	} else {
		buf := bytes.NewBuffer(b)
		dec := gob.NewDecoder(buf)
		ai :=new(ActorInfo)
		dec.Decode(&ai)
		return ai, nil
	}
}
func (c *Context) onDispatcherEnable() {

}
func (c *Context) onDispatcherDisable() {

}

func (c *Context) RegisterEncoder(name string,encoder func(value interface{})([]byte,error)){
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.encoders[name]=encoder
}
func (c *Context) RegisterDecoder(name string,decoder func(data []byte)(interface{},error)){
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.decoders[name]=decoder
}
func (c *Context) Encode(name string,value interface{})([]byte,error){
	encoder:=c.encoders[name]
	if encoder==nil{
		return nil,errors.New("encoder not found")
	}
	return encoder(value)
}
func (c *Context) Decode(name string,data []byte)(interface{},error){
	decoder:=c.decoders[name]
	if decoder==nil{
		return nil,errors.New("decoder not found")
	}
	return decoder(data)
}

type ActorInfo struct {
	SysName	string
	Addr 	string
}
func ActorInfoEncode(value *ActorInfo)([]byte,error){
	return GobEncode(value)
}
func ActorInfoDecode(data []byte)(interface{},error){
	info:=new(ActorInfo)
	return GobDecode(data,info)
}

var (
	ActorExistedError = errors.New("actor existed")
)
