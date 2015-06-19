package actor

import (
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"strconv"
	"strings"
)

type Lock struct {
	sys            *System
	lockPath, path string
}

func newLock(sys *System, path string) (*Lock, <-chan interface{}) {
	lock_path, _ := sys.ZKConn().Create(path+"/lock", []byte{}, zk.FlagPersistent, acl)
	p, _ := sys.ZKConn().Create(lock_path+"/", []byte{}, zk.FlagEphemeralSequential, acl)
	lock := &Lock{
		sys.ZKConn(),
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
	event := make(<-chan interface{})
	if index == 0 {
		event <- new(interface{})
		return lock, event
	}

	_, _, event, _ = sys.ZKConn().ExistsW(children[index-1])
	return lock, event
}

func (lock *Lock) release() {
	lock.sys.ZKConn().Delete(lock.path, int32(0))
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
	existed, _, _ := sys.ZKConn().Exists(path)
	return existed
}
func createDirectory(sys *System, path string) bool {
	conn := sys.ZKConn()
	if isExist(sys, path) {
		return false
	}

	parent := parentPath(path)
	if isExist(sys, parent) {
		createDirectory(sys, parent)
	}

	lock, event := newLock(sys, parent)
	defer lock.release()
	<-event
	if isExist(sys, path) {
		return false
	} else {
		conn.Create(path, []byte{}, zk.FlagPersistent, acl)
	}

	return true
}

func parentPath(path string) string {
	i := strings.LastIndex(path, "/")
	if i == 0 {
		return nil
	}
	return path[0:i]
}
