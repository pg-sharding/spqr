package pkg

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/wal-g/tracelog"
	"golang.yandex/hasql"
	"golang.yandex/hasql/checkers"
)

//TODO tests
type dbAction struct {
	id          uint64      `db:"id"`
	tableName   string      `db:"table_name"`
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
	actionsDBUser = "balancer"
	actionsDBName = "actions"
	//actionsDBName        = "postgres"
	//actionsDBUser        = "user1"
	actionsDBPassword    = ""
	actionsDBSslMode     = "disable"
	actionsDBSslRootCert = ""
	defaultSleepMS       = 1000
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
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?
	)`

	updateAction        = `update actions set action_stage = ?, is_running = ? where id = ? and table_name = ? and dbname = ?`
	markAllAsNotRunning = `update actions set is_running = false where table_name = ? and dbname = ?`
	deleteAction        = `delete from actions where id = ? and table_name = ? and dbname = ?`
	selectOneAction     = `select id, dbname, action_stage, is_running, left_bound, right_bound, shard_from, shard_to where is_running = FALSE and table_name = ? and dbname = ?`
	actionsCount        = `select count(*) from actions where is_running = false and table_name = ? and dbname = ?`
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

func NewCluster(addrs []string, dbname, user, password, sslMode, sslRootCert string, port int) (*hasql.Cluster, error) {
	nodes := make([]hasql.Node, 0, len(addrs))
	for _, addr := range addrs {
		connString := ConnString(addr, dbname, user, password, sslMode, sslRootCert, port)
		tracelog.InfoLogger.Printf("Connection string: %v", connString)
		node, err := sql.Open("pgx", connString)
		if err != nil {
			return nil, fmt.Errorf("failed to open pgx connection %v: %v", connString, err)
		}
		// TODO may be some connections settings here?

		nodes = append(nodes, hasql.NewNode(addr, node))
	}

	tracelog.InfoLogger.Printf("Nodes: %v", nodes)

	return hasql.NewCluster(nodes, checkers.PostgreSQL)
}

func ConnString(addr, dbname, user, password, sslMode, sslRootCert string, port int) string {
	var connParams []string

	host, portFromAddr, err := net.SplitHostPort(addr)
	if err == nil {
		connParams = append(connParams, "host="+host)
		connParams = append(connParams, "port="+portFromAddr)
	} else {
		connParams = append(connParams, "host="+addr)
		connParams = append(connParams, "port="+strconv.Itoa(port))
	}

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

	return strings.Join(connParams, " ")
}

func GetMasterConn(cluster *hasql.Cluster, retries int, sleepMS int) (*sql.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(defaultSleepMS))
	defer cancel()
	node, err := cluster.WaitForPrimary(ctx)
	if err != nil {
		return nil, fmt.Errorf("there is no node with role master: %s", err)
	}
	return GetNodeConn(node, retries, sleepMS)
}

func GetNodeConn(node hasql.Node, retries int, sleepMS int) (*sql.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(sleepMS))
	defer cancel()
	for i := 0; i < retries; i++ {

		err := node.DB().PingContext(ctx)
		if err != nil {
			fmt.Println("Master connection is dead")
			time.Sleep(time.Millisecond * time.Duration(sleepMS))
			continue
		}
		return node.DB(), nil
	}
	return nil, fmt.Errorf("failed to get connection with master node")
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
			6432,
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

	tx, err := conn.Begin()
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

	tx, err := conn.Begin()
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
		action.fromShard,
		action.toShard,
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
	tx, err := conn.Begin()
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

	tx, err := conn.Begin()
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
	tx, err := conn.Begin()
	if err != nil {
		fmt.Println(err)
		return Action{}, false, err
	}
	rows, err := tx.QueryContext(ctx, selectOneAction, d.tableName, d.dbname)
	if err != nil {
		_ = tx.Rollback()
		return Action{}, false, err
	}

	var act Action
	for rows.Next() {
		err = rows.Scan(&dbAct)
		if err != nil {
			return Action{}, false, err
		}
		act = dbAct.toAction()
		break
	}

	_, err = tx.Exec(updateAction, act.actionStage, true, act.id, d.tableName, d.dbname)
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return Action{}, false, err
	}

	err = tx.Commit()
	if err != nil {
		return Action{}, false, err
	}

	return Action{}, false, nil

}

func (d *Database) MarkAllNotRunning() error {
	conn, err := GetMasterConn(d.cluster, d.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.Begin()
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
	err = conn.QueryRow(actionsCount, d.tableName, d.dbname).Scan(&count)
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

func toPgHex(s string) string {
	hx := hex.EncodeToString([]byte(s))
	return fmt.Sprintf("\\x%s", hx)
}
