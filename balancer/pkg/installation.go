package pkg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
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
	createServer      = `create server %s foreign data wrapper postgres_fdw options (dbname '%s', host '%s', port '%d')`
	createUserMapping = `create user mapping if not exists for %s server %s options (user '%s', password '%s')`

	selectTableSchema = `select column_name, data_type from	information_schema.columns where table_name = $1`

	// TODO: comparing keys for: dblinkSubquery, deleteKeys, sampleRows.
	//  For example:
	//  where compare_like_numbers(?::bytea, key::bytea) >= 0 and compare_like_numbers(?::bytea, key::bytea) < 0')`.
	insertFromSelect = `insert into %s select k.* from dblink($1, $2) as k(%s);`
	dblinkSubquery   = `select * from %[1]s where %[2]s >= %[3]s and %[2]s < %[4]s`
	deleteKeys       = `delete from %[1]s where %[2]s >= %[3]s and %[2]s < %[4]s`
	sampleRows       = `select %[2]s from %[1]s where %[2]s >= %[3]s and %[2]s < %[4]s`
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
	Init(dbName, tableName, shardingKey, username, password string, shardClusters *map[int]*hasql.Cluster, retriesCount int) error
	GetShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error)
	StartTransfer(task Action) error
	RemoveRange(keyRange KeyRange, shard Shard) error
	GetKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error)
}

type Installation struct {
	dbName        string
	tableName     string
	shardingKey   string
	username      string
	password      string
	shardClusters *map[int]*hasql.Cluster
	retriesCount  int
}

func (i *Installation) Init(dbName, tableName, shardingKey, username, password string, shardClusters *map[int]*hasql.Cluster, retriesCount int) error {
	i.shardClusters = shardClusters
	i.retriesCount = retriesCount
	i.dbName = dbName
	i.shardingKey = shardingKey
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

var hardcodeShards = map[int]string{
	1: "spqr_shard_1_1:6432",
	2: "spqr_shard_2_1:6432",
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
	// hardcode shards to run locally
	host, port = AddrToHostPort(hardcodeShards[fromShard.id])

	_, err = tx.Exec(fmt.Sprintf(createServer, serverName, i.dbName, host, port))
	if err != nil {
		_ = tx.Rollback()
		fmt.Println(err)
		return err
	}

	_, err = tx.Exec(fmt.Sprintf(createUserMapping, i.username, serverName, i.username, i.password))
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

	defer rows.Close()

	var res []Column
	for rows.Next() {
		cur := Column{}
		err = rows.Scan(&cur.colName, &cur.colType)
		if err != nil {
			return nil, err
		}
		res = append(res, cur)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (i *Installation) StartTransfer(task Action) error {
	//TODO add dbname to serverName
	// Use only lower case names because PostgreSQL converts all SQL except string constants and identifiers in double quotes to lower case.
	serverName := fmt.Sprintf("serverdb%sshard%d", i.dbName, task.fromShard.id)

	err := i.prepareShardFDW(task.fromShard, task.toShard, serverName)
	if err != nil {
		return err
	}

	columns, err := i.GetTableSchema(task.fromShard)
	if err != nil {
		return err
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

	tableName := pgx.Identifier{i.tableName}.Sanitize()
	shardingKey := pgx.Identifier{i.shardingKey}.Sanitize()
	columnTypes := make([]string, 0, len(columns))
	for _, col := range columns {
		columnTypes = append(columnTypes, fmt.Sprintf("%s %s", col.colName, col.colType))
	}
	schemaStr := strings.Join(columnTypes, ",")
	subQuery := fmt.Sprintf(dblinkSubquery, tableName, shardingKey, task.keyRange.left, task.keyRange.right)

	_, err = tx.Exec(fmt.Sprintf(insertFromSelect, tableName, schemaStr), serverName, subQuery)
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

	tableName := pgx.Identifier{i.tableName}.Sanitize()
	shardingKey := pgx.Identifier{i.shardingKey}.Sanitize()

	_, err = tx.Exec(fmt.Sprintf(deleteKeys, tableName, shardingKey, keyRange.left, keyRange.right))
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
	tableName := pgx.Identifier{i.tableName}.Sanitize()
	shardingKey := pgx.Identifier{i.shardingKey}.Sanitize()

	rows, err := conn.QueryContext(context.Background(), fmt.Sprintf(sampleRows, tableName, shardingKey, keyRange.left, keyRange.right))
	if err != nil {
		return nil, err
	}
	var key Key
	// TODO: values greatestKey and count are hardcoded to avoid panic
	greatestKey := keyRange.right
	firstOne := true
	count := 3
	//greatestKey := ""
	//firstOne := true
	//count := 1
	for rows.Next() {
		break // TODO: remove after debugging
		err = rows.Scan(&key.key)
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
		//return big.NewInt(math.MaxInt64), nil // TODO: uncomment when sampleRows query works
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
