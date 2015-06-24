package actor

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"strconv"
	"strings"
	"log"
)

type Lock struct {
	sys            *System
	lockPath, path string
}

var (
	ZKParentDirectoryNotExistedError = errors.New("ZKParentDirectoryNotExistedError")
)

func sequent(sys *System, path string) (leader string,seqPath string, event <-chan zk.Event,err error) {
	parent:=parentPath(path)
	if parent!="" {
		existed, _, zkErr := sys.ZKConn().Exists(parent)
		if zkErr != nil {
			err=zkErr
			return
		}
		if existed == false {
			err = ZKParentDirectoryNotExistedError
			return
		}
	}

	seqPath, err = sys.ZKConn().Create(path, []byte{}, zk.FlagEphemeralSequential, acl)
	if err != nil {
		return
	}

	children, _, _ := sys.ZKConn().Children(parent)
	seq := Seq{children}
	sort.Sort(seq)
	leader=parent+"/"+children[0]
	index := 0
	for i, l := range children {
		if parent+"/"+l == seqPath {
			index = i
			break
		}
	}
	if index>0 {
		_, _, event, _ = sys.ZKConn().ExistsW(children[index-1])
	}
	return

}
var dummyEvent = zk.Event{}
func lockDirectory(sys *System, path string) (*Lock, <- chan zk.Event) {
	lock_path, _ := sys.ZKConn().Create(path+"/lock", []byte{}, zk.FlagPersistent, acl)
	p, _ := sys.ZKConn().Create(lock_path+"/", []byte{}, zk.FlagEphemeralSequential, acl)
	lock := &Lock{
		sys,
		lock_path,
		p,
	}

	children, _, _ := sys.ZKConn().Children(lock.lockPath)
	seq := Seq{children}
	sort.Sort(seq)
	index := 0
	for i, l := range children {
		if l == lock_path {
			index = i
			break
		}
	}

	if index == 0 {
		return lock, nil
	}

	_, _, event, _ := sys.ZKConn().ExistsW(children[index-1])
	return lock, event
}

func (lock *Lock) release() {
	lock.sys.ZKConn().Delete(lock.path, int32(0))
	parent:=parentPath(lock.path)
	if parent!="" {
		lock.sys.ZKConn().Delete(parentPath(lock.path), int32(0))
	}
}

type Seq struct {
	seq []string
}

func (s Seq) Len() int {
	return len(s.seq)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (s Seq) Less(i, j int) bool {
	vi, _ := strconv.Atoi(s.seq[i])
	vj, _ := strconv.Atoi(s.seq[j])
	return vi < vj
}

// Swap swaps the elements with indexes i and j.
func (s Seq) Swap(i, j int) {
	t := s.seq[i]
	s.seq[i] = s.seq[j]
	s.seq[j] = t
}

func isExist(sys *System, path string) bool {
	existed, _, err := sys.ZKConn().Exists(path)
	if err!=nil {
		panic(err)
	}
	return existed
}
func createDirectory(sys *System, path string) bool {
	conn := sys.ZKConn()
	if isExist(sys, path) {
		return false
	}

	parent := parentPath(path)
	if isExist(sys, parent)==false {
		createDirectory(sys, parent)
	}

	lock, event := lockDirectory(sys, parent)
	defer lock.release()
	if event!=nil {
		<-event
	}

	if isExist(sys, path) {
		return false
	} else {
		path,err:=conn.Create(path, []byte{}, zk.FlagPersistent, acl)
		if err!=nil {
			panic(err)
		}
		log.Println(path,"created")
	}

	return true
}

func createEphemeralDirectory(sys *System, path string) bool {
	conn := sys.ZKConn()
	if isExist(sys, path) {
		return false
	}

	parent := parentPath(path)
	if isExist(sys, parent)==false {
		createDirectory(sys, parent)
	}

	lock, event := lockDirectory(sys, parent)
	defer lock.release()
	if event!=nil {
		<-event
	}

	if isExist(sys, path) {
		return false
	} else {
		path,err:=conn.Create(path, []byte{}, zk.FlagEphemeral, acl)
		if err!=nil {
			panic(err)
		}
		log.Println(path,"created")
	}

	return true
}

func parentPath(path string) string {
	i := strings.LastIndex(path, "/")
	if i == 0 {
		return ""
	}
	return path[0:i]
}
