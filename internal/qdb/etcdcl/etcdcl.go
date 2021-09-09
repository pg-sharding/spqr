package etcdcl

import (
	"github.com/pg-sharding/spqr/internal/qdb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Qdbetcd struct {
	cli *clientv3.Client
}

func (q Qdbetcd) Watch(krid string, status qdb.KeyRangeStatus, notifyio chan<- interface{}) error {
	return nil
}

func NewQDBETCD() *Qdbetcd {

	cli, _ := clientv3.New(clientv3.Config{
		DialTimeout: 10,
		Endpoints:   []string{"127.0.0.1:2379"},
	})

	etcddb := &Qdbetcd{
		cli: cli,
	}

	return etcddb
}

func (q Qdbetcd) Lock(keyRange qdb.KeyRange) error {
	_, err := concurrency.NewSession(q.cli)

	return err
}

func (q Qdbetcd) UnLock(keyRange qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Add(keyRange qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Update(keyRange qdb.KeyRange) error {
	panic("implement me")
}

func (q Qdbetcd) Begin() error {
	panic("implement me")
}

func (q Qdbetcd) Commit() error {
	panic("implement me")
}

func (q Qdbetcd) Check(kr qdb.KeyRange) bool {
	return true
}

var _ qdb.QrouterDB = Qdbetcd{}
