package core

import (
	"container/list"
)

type clientPool struct {
	l *list.List
}

func (c * clientPool) pushcl(client *ShClient) *list.Element {
	return c.l.PushBack(client)
}

func (c * clientPool) popcl(e * list.Element) {
	c.l.Remove(e)
}