package pkg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"golang.yandex/hasql"
	"golang.yandex/hasql/checkers"
)

// TODO tests
type dbAction struct {
	id          uint64      `db:"id"`
	dbname      string      `db:"dbname"`
	actionStage ActionStage `db:"action_stage"`
	isRunning   bool        `db:"is_running"`
	leftBound   string      `db:"left_bound"`
	rightBound  string      `db:"right_bound"`
	shardFrom   int         `db:"shard_from"`
	shardTo     int         `db:"shard_to"`
}

func (a *dbAction) toAction() Action {
	return Action{
		id:          a.id,
		actionStage: a.actionStage,
		isRunning:   a.isRunning,
		keyRange:    KeyRange{left: a.leftBound, right: a.rightBound},
		fromShard:   Shard{id: a.shardFrom},
		toShard:     Shard{id: a.shardTo},
	}
}

type DatabaseInterface interface {
	Init(addrs []string, retriesCount int, dbname, tableName, actionsDBPassword string) error
	Insert(action *Action) error
	Update(action *Action) error
	Delete(action *Action) error
	GetAndRun() (Action, bool, error)
	MarkAllNotRunning() error
	Len() (uint64, error)
}

//TODO retries? not sure if required

var (
	//actionsDBUser = "balancer"
	//actionsDBName = "actions"
	actionsDBName        = "postgres"
	actionsDBUser        = "user1"
	actionsDBSslMode     = "disable"
	actionsDBSslRootCert = ""
	defaultSleepMS       = 10000
	defaultPort          = 5432

	//TODO add table and db to actions table? Current configuration will crash on many installation with many tables/databases
	tableActionsCreate = `
	create table if not exists actions (
		id SERIAL,
		table_name varchar(64),
		dbname varchar(64),
		action_stage INTEGER,
		is_running BOOLEAN,
		left_bound bytea,
		right_bound bytea,
		shard_from INTEGER,
		shard_to INTEGER
	)`

	insertAction = `
	insert into actions (
	    table_name,
		dbname,
		action_stage,
		is_running,
		left_bound,
		right_bound,
		shard_from,
		shard_to
	) values (
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7,
		$8
	)`

	updateAction        = `update actions set action_stage = $1, is_running = $2 where id = $3 and table_name = $4 and dbname = $5`
	markAllAsNotRunning = `update actions set is_running = false where table_name = $1 and dbname = $2`
	deleteAction        = `delete from actions where id = $1 and table_name = $2 and dbname = $3`

	takeActionForProcessing = `with cte as (select id from actions where is_running = false and table_name = $1 and dbname = $2 limit 1)
update actions a set is_running = true from cte where a.id = cte.id returning a.id, dbname, action_stage, is_running, left_bound, right_bound, shard_from, shard_to`
	actionsCount = `select count(*) from actions where is_running = false and table_name = $1 and dbname = $2`
)

type Database struct {
	cluster *hasql.Cluster

	dbname       string
	tableName    string
	retriesCount int
}

func AddrToHostPort(addr string) (string, int) {
	s := strings.Split(addr, ":")
	if len(s) == 1 {
		return s[0], defaultPort
	}
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return s[0], defaultPort
	}
	return s[0], port
}

func NewCluster(addrs []string, dbname, user, password, sslMode, sslRootCert string) (*hasql.Cluster, error) {
	nodes := make([]hasql.Node, 0, len(addrs))
	for _, addr := range addrs {
		connString, err := ConnString(addr, dbname, user, password, sslMode, sslRootCert)
		if err != nil {
			return nil, err
		}
		spqrlog.Logger.Printf(spqrlog.INFO, "Connection string: %v", connString)

		db, err := sql.Open("pgx", connString)
		if err != nil {
			return nil, fmt.Errorf("failed to open pgx connection %v: %v", connString, err)
		}
		// TODO may be some connections settings here?

		nodes = append(nodes, hasql.NewNode(addr, db))
	}

	spqrlog.Logger.Printf(spqrlog.INFO, "Nodes: %v", nodes)

	return hasql.NewCluster(nodes, checkers.PostgreSQL)
}

func ConnString(addr, dbname, user, password, sslMode, sslRootCert string) (string, error) {
	var connParams []string

	host, portFromAddr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid host spec: %s", err)
	}
	connParams = append(connParams, "host="+host)
	connParams = append(connParams, "port="+portFromAddr)

	if dbname != "" {
		connParams = append(connParams, "dbname="+dbname)
	}

	if user != "" {
		connParams = append(connParams, "user="+user)
	}

	if password != "" {
		connParams = append(connParams, "password="+password)
	}

	if sslRootCert != "" {
		connParams = append(connParams, "sslrootcert="+sslRootCert)
		//if CA cert is present and mode not specified then verify-full
		if sslMode == "" {
			sslMode = "verify-full"
		}
	}

	if sslMode != "" {
		connParams = append(connParams, "sslmode="+sslMode)
	} else {
		connParams = append(connParams, "sslmode=require")
	}

	return strings.Join(connParams, " "), nil
}

func GetMasterConn(cluster *hasql.Cluster, retries int, sleepMS int) (*sql.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(defaultSleepMS))
	defer cancel()
	node, err := cluster.WaitForPrimary(ctx)
	if err != nil {
		return nil, fmt.Errorf("there is no node with role master: %s", err)
	}
	return GetNodeConn(context.TODO(), node, retries, sleepMS)
}

func GetNodeConn(parentctx context.Context, node hasql.Node, retries int, sleepMS int) (*sql.Conn, error) {
	ctx, cancel := context.WithTimeout(parentctx, time.Millisecond*time.Duration(sleepMS))
	defer cancel()

	conn, err := node.DB().Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection with master node: %v", err)
	}

	return conn, nil
}

func (d *Database) Init(addrs []string, retriesCount int, dbname, tableName, actionsDBPassword string) error {
	cluster, err :=
		NewCluster(
			addrs,
			actionsDBName,
			actionsDBUser,
			actionsDBPassword,
			actionsDBSslMode,
			actionsDBSslRootCert,
		)
	if err != nil {
		return err
	}

	d.cluster = cluster
	d.dbname = dbname
	d.tableName = tableName
	d.retriesCount = retriesCount

	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(tableActionsCreate)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (d *Database) Insert(action *Action) error {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(insertAction,
		d.tableName,
		d.dbname,
		action.actionStage,
		action.isRunning,
		action.keyRange.left,
		action.keyRange.right,
		action.fromShard.id,
		action.toShard.id,
	)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (d *Database) Update(action *Action) error {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(updateAction, action.actionStage, action.isRunning, action.id, d.tableName, d.dbname)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (d *Database) Delete(action *Action) error {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(deleteAction, action.id, d.tableName, d.dbname)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (d *Database) GetAndRun() (Action, bool, error) {
	ctx := context.Background()
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return Action{}, false, err
	}
	defer conn.Close()

	dbAct := dbAction{}
	row := conn.QueryRowContext(ctx, takeActionForProcessing, d.tableName, d.dbname)

	err = row.Scan(
		&dbAct.id,
		&dbAct.dbname,
		&dbAct.actionStage,
		&dbAct.isRunning,
		&dbAct.leftBound,
		&dbAct.rightBound,
		&dbAct.shardFrom,
		&dbAct.shardTo,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println(err)
			return Action{}, false, nil
		}
		fmt.Println(err)
		return Action{}, false, fmt.Errorf("failed to grip an action row: %w", err)
	}

	if err := row.Err(); err != nil {
		fmt.Println(err)
		return Action{}, false, fmt.Errorf("row error: %w", err)
	}

	return dbAct.toAction(), true, nil

}

func (d *Database) MarkAllNotRunning() error {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(markAllAsNotRunning, d.tableName, d.dbname)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (d *Database) Len() (uint64, error) {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	count := uint64(0)
	err = conn.QueryRowContext(context.TODO(), actionsCount, d.tableName, d.dbname).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

type MockDb struct {
	actions map[uint64]Action
	count   uint64

	lock sync.Mutex
}

func (m *MockDb) Len() (uint64, error) {
	return m.count, nil
}

func (m *MockDb) MarkAllNotRunning() error {
	defer m.lock.Unlock()
	m.lock.Lock()
	for k, e := range m.actions {
		e.isRunning = false
		m.actions[k] = e
	}
	return nil
}

func (m *MockDb) Init(addrs []string, retriesCount int, _, _, _ string) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	m.actions = map[uint64]Action{}
	m.count = 0
	return nil
}

func (m *MockDb) Insert(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if ok {
		return errors.New(fmt.Sprint("Already in db: ", action))
	}
	maxId := uint64(0)

	for a := range m.actions {
		if a > maxId {
			maxId = a
		}
	}
	action.id = maxId + 1
	m.actions[action.id] = *action
	m.count += 1
	return nil
}

func (m *MockDb) Update(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if !ok {
		return errors.New(fmt.Sprint("Action not in db: ", action))
	}
	m.actions[action.id] = *action
	return nil
}

func (m *MockDb) Delete(action *Action) error {
	defer m.lock.Unlock()
	m.lock.Lock()
	_, ok := m.actions[action.id]
	if !ok {
		return errors.New(fmt.Sprint("Action not in db: ", action))
	}
	if m.actions[action.id].actionStage != actionStageDone {
		return errors.New(fmt.Sprint("Action stage shoud be actionStageDone, but: ", action.actionStage))
	}
	m.count -= 1
	delete(m.actions, action.id)
	return nil
}

func (m *MockDb) GetAndRun() (Action, bool, error) {
	defer m.lock.Unlock()
	m.lock.Lock()
	for k, e := range m.actions {
		if !e.isRunning {
			e.isRunning = true
			m.actions[k] = e
			return e, true, nil
		}
	}

	return Action{}, false, nil
}
