package pkg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"golang.yandex/hasql"
)

//TODO mb mark broken shards/transfers in db?

//TODO tests

var (
	createCompareFunction = `
CREATE OR REPLACE FUNCTION compare_like_numbers(a bytea, b bytea) RETURNS integer AS 
$$
BEGIN
    IF length(a) < length(b) OR (length(a) = length(b) and a < b) THEN
        return -1;
    END IF;
    IF length(a) > length(b) OR (length(a) = length(b) and b < a) THEN
        return 1;
    END IF;
    return 0;
END;
$$ LANGUAGE plpgsql;
`

	selectStatsFromRange = `select comment_keys->>'key' as key, reads, writes, user_time, system_time from pgcs_get_stats() where 
		compare_like_numbers(?::bytea, comment_keys->>'key'::bytea) >= 0 and 
		compare_like_numbers(comment_keys->>'key'::bytea, ?::bytea) < 0`

	dropServer        = "drop server if exists %s cascade;"
	createServer      = `CREATE server %s foreign data wrapper postgres_fdw OPTIONS (dbname '%s', host '%s', port '%d')`
	createUserMapping = `CREATE USER MAPPING IF NOT EXISTS FOR %s SERVER %s OPTIONS (user '%s', password '%s')`

	selectTableSchema = `SELECT
		column_name,
		data_type
	FROM
		information_schema.columns
	WHERE
		table_name = $1`
	insertFromSelect = `insert into ? select k.* from dblink(?, 'Select * From keys where compare_like_numbers(?::bytea, key::bytea) >= 0 and compare_like_numbers(?::bytea, key::bytea) < 0') as k(?);`

	deleteKeys = `delete from ? where compare_like_numbers(?::bytea, key::bytea) >= 0 and compare_like_numbers(?::bytea, key::bytea) < 0`
	// TODO: the original query fails
	//sampleRows = `select key from ? where compare_like_numbers(?::bytea, key::bytea) >= 0 and compare_like_numbers(?::bytea, key::bytea) < 0`
	sampleRows = `select id from x where $1 <= id and $2 > id`
)

type Column struct {
	colType string `db:"data_type"`
	colName string `db:"column_name"`
}

type Key struct {
	key string `db:"key"`
}

type DBStats struct {
	Key
	Stats
}

func (s *DBStats) toStats() Stats {
	return Stats{
		reads:      s.reads,
		writes:     s.writes,
		systemTime: s.systemTime,
		userTime:   s.userTime,
	}
}

type InstallationInterface interface {
	Init(dbName, tableName, username, password string, shardClusters *map[int]*hasql.Cluster, retriesCount int) error
	GetShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error)
	StartTransfer(task Action) error
	RemoveRange(keyRange KeyRange, shard Shard) error
	GetKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error)
}

type Installation struct {
	dbName        string
	tableName     string
	username      string
	password      string
	shardClusters *map[int]*hasql.Cluster
	retriesCount  int
}

func (i *Installation) Init(dbName, tableName, username, password string, shardClusters *map[int]*hasql.Cluster, retriesCount int) error {
	i.shardClusters = shardClusters
	i.retriesCount = retriesCount
	i.dbName = dbName
	i.username = username
	i.tableName = tableName
	i.password = password
	for _, shardCluster := range *shardClusters {
		conn, err := GetMasterConn(shardCluster, retriesCount, defaultSleepMS)
		if err != nil {
			fmt.Println(err)
			continue
		}

		tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
		if err != nil {
			fmt.Println(err)
			_ = conn.Close()
			continue
		}
		_, err = tx.Exec(createCompareFunction)
		if err != nil {
			_ = tx.Rollback()
			fmt.Println(err)
			_ = conn.Close()
			continue
		}
		err = tx.Commit()
		if err != nil {
			fmt.Println(err)
			_ = conn.Close()
			continue
		}
		_ = conn.Close()
	}
	return nil
}

func (i *Installation) GetRangeStats(ctx context.Context, conn *sql.Conn, keyRange KeyRange) (map[string]map[string]Stats, error) {
	rows, err := conn.QueryContext(ctx, selectStatsFromRange, keyRange.left, keyRange.right)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]Stats{}
	result[keyRange.left] = map[string]Stats{}
	dbStats := DBStats{}
	for rows.Next() {
		err = rows.Scan(&dbStats)
		if err != nil {
			return nil, err
		}
		_, ok := result[keyRange.left][dbStats.key]
		if !ok {
			result[keyRange.left][dbStats.key] = Stats{}
		}
		result[keyRange.left][dbStats.key] = AddStats(result[keyRange.left][dbStats.key], dbStats.toStats())
	}
	return result, nil
}

func (i *Installation) GetHostStats(node hasql.Node, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	result := map[string]map[string]Stats{}

	conn, err := GetNodeConn(context.TODO(), node, i.retriesCount, defaultSleepMS)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx := context.Background()
	for _, keyRange := range keyRanges {
		rangeStats, err := i.GetRangeStats(ctx, conn, keyRange)
		if err != nil {
			return nil, err
		}
		AddHostStats(&result, &rangeStats)
	}
	return result, nil
}

func AddHostStats(stats *map[string]map[string]Stats, additionalStats *map[string]map[string]Stats) {
	for leftBorder := range *additionalStats {
		for border := range (*additionalStats)[leftBorder] {
			_, ok := (*stats)[leftBorder]
			if !ok {
				(*stats)[leftBorder] = map[string]Stats{}
			}
			_, ok = (*stats)[leftBorder][border]
			if !ok {
				(*stats)[leftBorder][border] = Stats{}
			}
			AddStats((*stats)[leftBorder][border], (*additionalStats)[leftBorder][border])
		}
	}
}

func (i *Installation) GetShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	cluster, ok := (*i.shardClusters)[shard.id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Not known shard %d", shard.id))
	}

	nodes := cluster.Nodes()
	var res map[string]map[string]Stats
	for _, node := range nodes {
		hostStats, err := i.GetHostStats(node, keyRanges)
		if err != nil {
			fmt.Println(fmt.Sprintf("Can't read stats from node %s because of %s", node, err))
			continue
		}
		AddHostStats(&res, &hostStats)
	}
	return nil, nil
}

func (i *Installation) prepareShardFDW(fromShard Shard, toShard Shard, serverName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(defaultSleepMS))
	defer cancel()
	sourceMaster, err := (*i.shardClusters)[fromShard.id].WaitForPrimary(ctx)
	if err != nil {
		return err
	}
	conn, err := GetMasterConn((*i.shardClusters)[toShard.id], i.retriesCount, defaultSleepMS)
	if err != nil {
		fmt.Println(fmt.Sprintf("Can't find master of shard %d: %s", fromShard.id, err))
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(dropServer, serverName))
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}

	// split sourceMaster.Addr() by : to host and port
	host, port := AddrToHostPort(sourceMaster.Addr())
	_, err = tx.Exec(fmt.Sprintf(createServer, serverName, i.dbName, host, port))

	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}

	_, err = tx.Exec(fmt.Sprintf(createUserMapping,
		i.username,
		serverName,
		i.username,
		i.password))

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (i *Installation) GetTableSchema(shard Shard) ([]Column, error) {
	conn, err := GetMasterConn((*i.shardClusters)[shard.id], i.retriesCount, defaultSleepMS)
	if err != nil {
		fmt.Println(fmt.Sprintf("Can't find master of shard %d: %s", shard.id, err))
	}
	defer conn.Close()
	rows, err := conn.QueryContext(context.Background(), selectTableSchema, i.tableName)
	if err != nil {
		return nil, err
	}
	var res []Column
	for rows.Next() {
		cur := Column{}
		err = rows.Scan(&cur)
		if err != nil {
			return nil, err
		}
		res = append(res, cur)
	}

	return res, nil
}

func (i *Installation) StartTransfer(task Action) error {
	//TODO add dbname to serverName
	serverName := fmt.Sprintf("serverDB%sShard%d", i.dbName, task.fromShard.id)

	err := i.prepareShardFDW(task.fromShard, task.toShard, serverName)
	if err != nil {
		return err
	}

	columns, err := i.GetTableSchema(task.fromShard)
	if err != nil {
		return err
	}

	schemaStr := ""
	first := true
	for _, col := range columns {
		if !first {
			schemaStr += ", "
		}
		first = false
		schemaStr += fmt.Sprintf("%s %s", col.colName, col.colType)
	}

	conn, err := GetMasterConn((*i.shardClusters)[task.toShard.id], i.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}

	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return err
	}
	_, err = tx.Exec(insertFromSelect, i.tableName, serverName, task.keyRange.left, task.keyRange.right, schemaStr)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (i *Installation) RemoveRange(keyRange KeyRange, shard Shard) error {
	conn, err := GetMasterConn((*i.shardClusters)[shard.id], i.retriesCount, defaultSleepMS)
	if err != nil {
		return err
	}

	defer conn.Close()

	tx, err := conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return err
	}

	_, err = tx.Exec(deleteKeys, i.tableName, keyRange.left, keyRange.right)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (i *Installation) GetKeyDistanceByRange(conn *sql.Conn, keyRange KeyRange) (*big.Int, error) {
	//i.tableName
	rows, err := conn.QueryContext(context.Background(), sampleRows, keyRange.left, keyRange.right)
	if err != nil {
		return nil, err
	}
	var key Key
	// TODO: values greatestKey and count are hardcoded to avoid panic
	greatestKey := keyRange.right
	firstOne := true
	count := 3
	for rows.Next() {
		err = rows.Scan(&key)
		if err != nil {
			return nil, err
		}
		if firstOne || less(&greatestKey, &key.key) {
			greatestKey = key.key
		}
		count += 1
		firstOne = false
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("failed to close keyRange rows: %w", err)
	}

	kr := KeyRange{
		left:  keyRange.left,
		right: greatestKey,
	}
	if count == 0 {
		//return big.NewInt(math.MaxInt64), nil // TODO: uncomment when sampleRows query is working
	}

	return new(big.Int).Div(getLength(kr), big.NewInt(int64(count))), nil
}

func (i *Installation) GetKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error) {
	res := map[string]*big.Int{}

	conn, err := GetMasterConn((*i.shardClusters)[shard.id], i.retriesCount, defaultSleepMS)
	if err != nil {
		return nil, err
	}

	defer conn.Close()
	for _, kr := range keyRanges {
		d, err := i.GetKeyDistanceByRange(conn, kr)
		if err != nil {
			return nil, err
		}
		res[kr.left] = d
	}
	return res, nil
}
