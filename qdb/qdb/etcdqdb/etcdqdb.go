package etcdqdb

import (
	"context"
	"encoding/json"
	"path"

	"github.com/pg-sharding/spqr/qdb/qdb"
	"github.com/wal-g/tracelog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
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

type EtcdQDB struct {
	qdb.QrouterDB

	cli *clientv3.Client

	locks map[string]*notifier
}

const keyRangesNamespace = "/keyranges"
const routersRangesNamespace = "/routers"

func (q EtcdQDB) DropKeyRange(keyRange *qdb.KeyRange) error {
	resp, err := q.cli.Delete(context.TODO(), path.Join(keyRangesNamespace, keyRange.KeyRangeID))

	tracelog.InfoLogger.Printf("delete resp %v", resp)
	return err
}

func (q EtcdQDB) ListRouters() ([]*qdb.Router, error) {
	resp, err := q.cli.Get(context.TODO(), routersRangesNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Printf("got resp %v", resp)
	var ret []*qdb.Router

	for _, e := range resp.Kvs {
		_, keyRangeID := path.Split(string(e.Key))
		ret = append(ret,
			qdb.NewRouter(
				string(e.Value),
				keyRangeID,
			),
		)
	}

	return ret, nil
}

func (q EtcdQDB) Watch(krid string, status *qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	return nil
}

const keyspace = "worldmock"

func NewEtcdQDB(addr string) (*EtcdQDB, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
		},
	})

	if err != nil {
		return nil, err
	}

	return &EtcdQDB{
		cli:   cli,
		locks: map[string]*notifier{},
	}, nil
}

func (q EtcdQDB) AddRouter(r *qdb.Router) error {
	resp, err := q.cli.Put(context.TODO(), "/routers/"+r.ID(), r.Addr())
	tracelog.InfoLogger.Printf("put resp %v", resp)
	return err
}

func (q EtcdQDB) Lock(keyRangeID string) (*qdb.KeyRange, error) {
	sess, err := concurrency.NewSession(q.cli)
	if err != nil {
		return nil, err
	}

	mu := concurrency.NewMutex(sess, keyspace)

	go func(mutex *concurrency.Mutex) {
		mutex.Unlock(context.TODO())
		q.locks[keyRangeID].nofity(struct {
		}{})
	}(mu)

	return nil, err
}

func (q EtcdQDB) UnLock(keyRange string) error {
	panic("implement me")
}

func (q EtcdQDB) AddKeyRange(keyRange *qdb.KeyRange) error {
	rawKeyRange, err := json.Marshal(keyRange)

	resp, err := q.cli.Put(context.TODO(), "/keyranges/"+keyRange.KeyRangeID, string(rawKeyRange))
	tracelog.InfoLogger.Printf("put resp %v", resp)
	return err
}

func (q EtcdQDB) Check(kr *qdb.KeyRange) bool {
	return true
}

var _ qdb.QrouterDB = EtcdQDB{}
