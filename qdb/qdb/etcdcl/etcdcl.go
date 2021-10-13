package etcdcl

import (
	"context"

	"github.com/pg-sharding/spqr/qdb/qdb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type notifier struct {
	pch chan interface{}
	sch chan chan interface{}
}

func newnotifier() *notifier {
	return &notifier{
		pch: make(chan interface{}, 1),
		sch: make(chan chan interface{}, 1),
	}
}

func (n *notifier) run() {
	subs := map[chan interface{}]struct{}{}
	for {
		select {
		case msgCh := <-n.sch:
			subs[msgCh] = struct{}{}
		case msg := <-n.pch:
			for msgCh := range subs {
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

func (n *notifier) Subscribe() chan interface{} {
	msgCh := make(chan interface{}, 0)
	n.sch <- msgCh
	return msgCh
}

func (n *notifier) nofity(msg interface{}) {
	n.pch <- msg
}

type Qdbetcd struct {
	cli *clientv3.Client

	locks map[string]*notifier
}

func (q Qdbetcd) Watch(krid string, status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	return nil
}

const keyspace = "router"

func NewQDBETCD() (*Qdbetcd, error) {
	cli, _ := clientv3.New(clientv3.Config{ // TODO error handling
		DialTimeout: 10,
		Endpoints:   []string{"127.0.0.1:2379"},
	})

	return &Qdbetcd{
		cli:   cli,
		locks: map[string]*notifier{},
	}, nil
}

func (q Qdbetcd) Lock(keyRange *qdb.KeyRange) error {
	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return err
	}

	mu := concurrency.NewMutex(sess, keyspace)

	go func(mutex *concurrency.Mutex) {
		mutex.Unlock(context.TODO())
		q.locks[keyRange.KeyRangeID].nofity(struct {
		}{})
	}(mu)

	return err
}

func (q Qdbetcd) UnLock(keyRange *qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Add(keyRange *qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Update(keyRange *qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Begin() error {
	panic("implement me")
}

func (q Qdbetcd) Commit() error {
	panic("implement me")
}

func (q Qdbetcd) Check(kr *qdb.KeyRange) bool {
	return true
}

var _ qdb.QrouterDB = Qdbetcd{}
