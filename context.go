package actor

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
	"sync"
)

type ActorNoFoundError struct {
	path string
}

func (e ActorNoFoundError) Error() string {
	return "actor " + e.path + " not found"
}

type Context struct {
	system *System
	conf   Config
	actors map[string]Actor
	mutex  sync.Mutex
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
	go func() error {
		for {
			select {
			case msg := <-a.mailbox():
				a.Receive(msg)
			case e := <-a.events():
				switch e {
				case EventStop:

					break
				}
			}

		}
	}()
}

func (c *Context) stopActor(a Actor) error {
	a.events() <- EventStop
	return a.PreStop()
}
func (c *Context) LocalActorOf(path string, _type reflect.Type, props map[string]interface{}) (*Actor, error) {
	newObjPtr := reflect.New(_type.Elem()).Interface()
	a := newObjPtr.(Actor)
	if err := a.Init(props); err != nil {
		return nil, err
	}
	c.actors[path] = a
	c.startActor(a)
	return a, nil
}
func (c *Context) ActorOf(path string, ty reflect.Type, props map[string]interface{}) (*Actor, error) {
	if a := c.actors[path]; a != nil {
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
	a := newObjPtr.(Actor)
	err = a.Init(props)
	return a, err
}

func (c *Context) ActorSelection(path string) *Actor {
	a := c.actors[path]
	if a == nil && isExist(c.system, path) {
		a = newRemoteActor(c.system, c, path)
	}
	return a
}

func (c *Context) readActorInfo(path string) (*ActorInfo, error) {
	zkConn := c.system.ZKConn()
	if b, _, err := zkConn.Get(path); err != nil {
		return nil, err
	} else {
		buf := bytes.NewBuffer(b)
		dec := gob.NewDecoder(buf)
		var ai ActorInfo
		dec.Decode(&ai)
		return ai, nil
	}
}

type ActorInfo struct {
	Addr string
}

var (
	ActorExistedError = errors.New("actor existed")
)
