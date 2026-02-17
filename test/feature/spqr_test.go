package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/router/rfqn"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/sethvargo/go-retry"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/test/feature/testutil"
	"github.com/pg-sharding/spqr/test/feature/testutil/matchers"
)

const (
	commandExecutionTimeout         = 10 * time.Second
	spqrShardName                   = "shard"
	spqrRouterName                  = "router"
	spqrCoordinatorName             = "coordinator"
	spqrQDBName                     = "qdb"
	spqrPort                        = 6432
	spqrConsolePort                 = 7432
	spqrCoordinatorPort             = 7002
	qdbPort                         = 2379
	shardUser                       = "regress"
	shardPassword                   = "12345678"
	coordinatorPassword             = "password"
	dbName                          = "regress"
	postgresqlConnectTimeout        = 60 * time.Second
	postgresqlInitialConnectTimeout = 30 * time.Second
	postgresqlQueryTimeout          = 10 * time.Second
	qdbQueriesTimeout               = 30 * time.Second

	spqrQdbHost             = "qdb01"
	checkCoordinatorTimeout = 15 * time.Second

	SERVICE_STATE_RUNNING = "running"
)

type testContext struct {
	variables         map[string]any
	templateErr       error
	composer          testutil.Composer
	composerEnv       []string
	userDbs           map[string]map[string]*sql.DB
	sqlQueryResult    []map[string]any
	sqlUserQueryError sync.Map // host -> error
	commandRetcode    int
	commandOutput     string
	qdb               qdb.XQDB
	t                 *testing.T
	debug             bool
	preparedQueries   map[string]map[string]*sql.Stmt
}

func newTestContext(t *testing.T) (*testContext, error) {
	var err error
	tctx := new(testContext)
	tctx.composer, err = testutil.NewDockerComposer("spqr", "docker-compose.yaml")
	tctx.t = t
	if err != nil {
		return nil, err
	}
	tctx.userDbs = make(map[string]map[string]*sql.DB)
	tctx.preparedQueries = make(map[string]map[string]*sql.Stmt)
	return tctx, nil
}

var postgresqlLogsToSave = map[string]string{
	"/var/log/postgresql/postgresql-13-main.log": "postgresql.log",
}

var routerLogsToSave = map[string]string{
	"/var/log/spqr-router.log": "router.log",
}

func (tctx *testContext) saveLogs(scenario string) error {
	var errs []error
	for _, service := range tctx.composer.Services() {
		var logsToSave map[string]string
		switch {
		case strings.HasPrefix(service, spqrShardName):
			logsToSave = postgresqlLogsToSave
		case strings.HasPrefix(service, spqrRouterName):
			logsToSave = routerLogsToSave
		default:
			continue
		}
		// Print logs to stdout when debug is enabled
		if tctx.debug {
			for file := range logsToSave {
				remoteFile, err := tctx.composer.GetFile(service, file)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				content, err := io.ReadAll(remoteFile)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				fmt.Printf("\"%s\" logs:\n", service)
				fmt.Println(string(content))
			}
		}

		logdir := filepath.Join("logs", scenario, service)
		err := os.MkdirAll(logdir, 0755)
		if err != nil {
			return err
		}
		for remotePath, localPath := range logsToSave {
			if strings.Contains(remotePath, "%") {
				remotePath = fmt.Sprintf(remotePath, service)
			}
			remoteFile, err := tctx.composer.GetFile(service, remotePath)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			defer func() { _ = remoteFile.Close() }()
			localFile, err := os.OpenFile(filepath.Join(logdir, localPath), os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			defer func() { _ = localFile.Close() }()
			_, err = io.Copy(localFile, remoteFile)
			if err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	if len(errs) > 0 {
		var msg strings.Builder
		for _, err := range errs {
			msg.WriteString(err.Error() + "\n")
		}
		return errors.New(msg.String())
	}
	return nil
}

func (tctx *testContext) templateStep(step *godog.Step) error {
	var err error
	step.Text, err = tctx.templateString(step.Text)
	if err != nil {
		return err
	}

	if step.Argument != nil {
		if step.Argument.DocString != nil {
			newContent, err := tctx.templateString(step.Argument.DocString.Content)
			if err != nil {
				return err
			}
			step.Argument.DocString.Content = newContent
		}
		if step.Argument.DataTable != nil {
			if step.Argument.DataTable.Rows != nil {
				for _, row := range step.Argument.DataTable.Rows {
					for _, cell := range row.Cells {
						cell.Value, err = tctx.templateString(cell.Value)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func (tctx *testContext) templateString(data string) (string, error) {
	if !strings.Contains(data, "{{") {
		return data, nil
	}
	tpl, err := template.New(data).Parse(data)
	if err != nil {
		return data, err
	}
	var res strings.Builder
	err = tpl.Execute(&res, tctx.variables)
	if err != nil {
		return data, err
	}
	return res.String(), nil
}

func (tctx *testContext) cleanup() {
	for _, dbs := range tctx.userDbs {
		for _, db := range dbs {
			if err := db.Close(); err != nil {
				log.Printf("failed to close db connection: %s", err)
			}
		}
	}
	tctx.userDbs = make(map[string]map[string]*sql.DB)
	if err := tctx.composer.Down(); err != nil {
		log.Printf("failed to tear down compose: %s", err)
	}
	tctx.variables = make(map[string]any)
	tctx.composerEnv = make([]string, 0)
	tctx.sqlQueryResult = make([]map[string]any, 0)
	tctx.sqlUserQueryError = sync.Map{}
	tctx.commandRetcode = 0
	tctx.commandOutput = ""
	tctx.closePreparedPostgresql()
}

func (tctx *testContext) connectPostgresql(addr string, user string, timeout time.Duration) (*sql.DB, error) {
	if strings.Contains(addr, strconv.Itoa(spqrConsolePort)) {
		return tctx.connectRouterConsoleWithCredentials(user, shardPassword, addr, timeout)
	}
	if strings.Contains(addr, strconv.Itoa(spqrCoordinatorPort)) {
		return tctx.connectRouterConsoleWithCredentials(user, coordinatorPassword, addr, timeout)
	}
	return tctx.connectPostgresqlWithCredentials(user, shardPassword, addr, timeout)
}

func (tctx *testContext) connectPostgresqlWithCredentials(username string, password string, addr string, timeout time.Duration) (*sql.DB, error) {
	ping := func(db *sql.DB) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := db.PingContext(ctx)
		if err != nil {
			log.Printf("failed to ping postgres at %s: %s", addr, err)
		}
		return err == nil
	}
	return tctx.connectorWithCredentials(username, password, addr, dbName, timeout, ping)
}

func (tctx *testContext) connectCoordinatorWithCredentials(username string, password string, addr string, timeout time.Duration) (*sql.DB, error) {
	ping := func(db *sql.DB) bool {
		_, err := db.Exec("SHOW routers")
		if err != nil {
			log.Printf("failed to ping coordinator at %s: %s", addr, err)
		}
		return err == nil
	}
	return tctx.connectorWithCredentials(username, password, addr, dbName, timeout, ping)
}

func (tctx *testContext) connectRouterConsoleWithCredentials(username string, password string, addr string, timeout time.Duration) (*sql.DB, error) {
	ping := func(db *sql.DB) bool {
		_, err := db.Exec("SHOW key_ranges")
		if err != nil {
			log.Printf("failed to ping router console at %s: %s", addr, err)
		}
		return err == nil
	}
	return tctx.connectorWithCredentials(username, password, addr, dbName, timeout, ping)
}

func (tctx *testContext) connectorWithCredentials(username string, password string, addr string, dbName string, timeout time.Duration, ping func(db *sql.DB) bool) (*sql.DB, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, addr, dbName)
	connCfg, _ := pgx.ParseConfig(dsn)
	connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connCfg.RuntimeParams["client_encoding"] = "UTF8"
	connCfg.RuntimeParams["standard_conforming_strings"] = "on"
	connStr := stdlib.RegisterConnConfig(connCfg)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}
	success := false
	// sql is lazy in go, so we need ping db
	testutil.Retry(func() bool {
		success = ping(db)
		return success
	}, timeout, 2*time.Second)
	if !success {
		return nil, fmt.Errorf("postgres does not respond")
	}
	return db, nil
}

func (tctx *testContext) trySetupConnectionRouter(user, service string) (*sql.DB, error) {
	routerService := strings.TrimSuffix(service, "-admin")
	adminService := routerService + "-admin"
	addrRouter, err := tctx.composer.GetAddr(service, spqrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get router addr %s: %s", routerService, err)
	}
	dbRouter, err := tctx.connectPostgresql(addrRouter, user, postgresqlInitialConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SPQR router %s: %s", routerService, err)
	}
	if _, ok := tctx.userDbs[user]; !ok {
		tctx.userDbs[user] = make(map[string]*sql.DB)
	}
	tctx.userDbs[user][routerService] = dbRouter

	// router console
	addrAdmin, err := tctx.composer.GetAddr(service, spqrConsolePort)
	if err != nil {
		return nil, fmt.Errorf("failed to get router addr %s: %s", adminService, err)
	}
	dbAdm, err := tctx.connectRouterConsoleWithCredentials(user, shardPassword, addrAdmin, postgresqlInitialConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SPQR router %s: %s", adminService, err)
	}
	if _, ok := tctx.userDbs[user]; !ok {
		tctx.userDbs[user] = make(map[string]*sql.DB)
	}
	tctx.userDbs[user][adminService] = dbAdm

	return tctx.userDbs[user][service], nil
}

func (tctx *testContext) trySetupConnection(user, service string) (*sql.DB, error) {
	// check databases
	if strings.HasPrefix(service, spqrShardName) {
		addr, err := tctx.composer.GetAddr(service, spqrPort)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard addr %s: %s", service, err)
		}
		db, err := tctx.connectPostgresql(addr, shardUser, postgresqlInitialConnectTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to postgresql %s: %s", service, err)
		}
		if _, ok := tctx.userDbs[user]; !ok {
			tctx.userDbs[user] = make(map[string]*sql.DB)
		}
		tctx.userDbs[user][service] = db
		return db, nil
	}

	// check router
	if strings.HasPrefix(service, spqrRouterName) {
		return tctx.trySetupConnectionRouter(user, service)
	}

	// check coordinator
	if strings.HasPrefix(service, spqrCoordinatorName) {
		addr, err := tctx.composer.GetAddr(service, spqrCoordinatorPort)
		if err != nil {
			return nil, fmt.Errorf("failed to get coordinator addr %s: %s", service, err)
		}
		db, err := tctx.connectCoordinatorWithCredentials(shardUser, coordinatorPassword, addr, postgresqlInitialConnectTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to SPQR coordinator %s: %s", service, err)
		}
		if _, ok := tctx.userDbs[user]; !ok {
			tctx.userDbs[user] = make(map[string]*sql.DB)
		}
		tctx.userDbs[user][service] = db
		return db, nil
	}

	return nil, fmt.Errorf("unrecognised service \"%s\"", service)
}

func (tctx *testContext) getPostgresqlConnection(user, host string) (*sql.DB, error) {
	var db *sql.DB
	dbs, ok := tctx.userDbs[user]
	if !ok {
		var err error
		db, err = tctx.trySetupConnection(user, host)
		if err != nil {
			return nil, fmt.Errorf("postgresql %s is not in cluster", host)
		}
	} else {
		db, ok = dbs[host]
		if !ok {
			var err error
			db, err = tctx.trySetupConnection(user, host)
			if err != nil {
				return nil, fmt.Errorf("postgresql %s is not in cluster", host)
			}
		}
	}
	if strings.HasSuffix(host, "admin") || strings.HasPrefix(host, "coordinator") {
		return db, nil
	}
	err := db.Ping()
	if err == nil {
		return db, nil
	}
	addr, err := tctx.composer.GetAddr(host, spqrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgresql addr %s: %s", host, err)
	}
	db, err = tctx.connectPostgresql(addr, user, postgresqlConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgresql %s: %s", host, err)
	}

	if _, ok := tctx.userDbs[user]; !ok {
		tctx.userDbs[user] = make(map[string]*sql.DB)
	}
	tctx.userDbs[user][host] = db
	return db, nil
}

func (tctx *testContext) prepareQueryPostgresql(host, user, query string) error {
	db, err := tctx.getPostgresqlConnection(user, host)
	if err != nil {
		return err
	}
	stmt, err := db.Prepare(query)
	if pqHst, ok := tctx.preparedQueries[host]; !ok {
		hstDat := map[string]*sql.Stmt{query: stmt}
		tctx.preparedQueries[host] = hstDat
	} else {
		pqHst[query] = stmt
		tctx.preparedQueries[host] = pqHst
	}
	tctx.sqlQueryResult = nil
	tctx.commandRetcode = 0
	if err != nil {
		tctx.commandRetcode = 1
		tctx.commandOutput = err.Error()
		tctx.sqlUserQueryError.Store(host, err.Error())
	}
	return nil
}

func (tctx *testContext) queryPreparedPostgresql(host, query string, args []any) ([]map[string]any, error) {
	tctx.sqlQueryResult = nil
	result, err := tctx.doPrepQueryPostgresql(host, query, args)
	tctx.commandRetcode = 0
	if err != nil {
		tctx.commandRetcode = 1
		tctx.commandOutput = err.Error()
		tctx.sqlUserQueryError.Store(host, err.Error())
	}
	tctx.sqlQueryResult = result
	return result, nil
}

func (tctx *testContext) closePreparedPostgresql() {
	for host, hostPrepared := range tctx.preparedQueries {
		for query, preparedQuery := range hostPrepared {
			if err := preparedQuery.Close(); err != nil {
				log.Printf("On host %s close prepared query '%s' error %#v\n", host, query, err)
			}
		}
	}
}

func (tctx *testContext) doPrepQueryPostgresql(host, query string, args []any) ([]map[string]any, error) {
	if stmts, ok := tctx.preparedQueries[host]; !ok {
		return nil, fmt.Errorf("Query '%s' is not prepared", query)
	} else {
		if stmt, ok := stmts[query]; !ok {
			return nil, fmt.Errorf("Query '%s' is not prepared", query)
		} else {
			rows, err := stmt.Query(args...)
			if err != nil {
				log.Printf("query error %#v\n", err)
				return nil, err
			}
			defer func() {
				_ = rows.Close()
			}()
			result := make([]map[string]any, 0)
			for rows.Next() {
				rowmap, err := testutil.CurrenRowToMap(rows)
				if err != nil {
					return nil, err
				}
				for k, v := range rowmap {
					if v2, ok := v.([]byte); ok {
						rowmap[k] = string(v2)
					}
				}
				result = append(result, rowmap)
			}
			return result, nil
		}
	}
}

func (tctx *testContext) queryPostgresql(host, user, query string, timeout time.Duration, args []any) ([]map[string]any, error) {
	db, err := tctx.getPostgresqlConnection(user, host)
	if err != nil {
		return nil, err
	}
	// sqlx is not used now. try remove split
	queries := strings.Split(query, ";")
	var result []map[string]any

	for _, q := range queries {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		tctx.sqlQueryResult = nil
		result, err = tctx.doPostgresqlQuery(db, q, timeout, args)
		tctx.commandRetcode = 0
		tctx.sqlQueryResult = result
		if err != nil {
			tctx.commandRetcode = 1
			tctx.commandOutput = err.Error()
			tctx.sqlUserQueryError.Store(host, err.Error())
			break
		}
	}

	return result, nil
}

func (tctx *testContext) executePostgresql(host string, query string) error {
	db, err := tctx.getPostgresqlConnection(shardUser, host)
	if err != nil {
		return err
	}

	// sqlx is not used now. try remove split
	queries := strings.SplitSeq(query, ";")

	for q := range queries {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tctx *testContext) stepIExecuteSqlInParallel(host string, timeout int, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(timeout)*time.Second)
	defer cancel()

	// sqlx is not used now. try remove split
	queries := strings.SplitSeq(query, ";")

	wg := sync.WaitGroup{}
	var execErr error
	for q := range queries {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		wg.Go(func() {
			db, err := tctx.getPostgresqlConnection(shardUser, host)
			if err != nil {
				execErr = err
				return
			}
			_, err = db.QueryContext(ctx, q)
			if err != nil {
				execErr = err
			}
		})
	}
	wg.Wait()
	return execErr
}

func (tctx *testContext) doPostgresqlQuery(db *sql.DB, query string, timeout time.Duration, args []any) ([]map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var rows *sql.Rows
	var err error
	rows, err = db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Printf("query error %#v\n", err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	result := make([]map[string]any, 0)
	for rows.Next() {
		rowmap, err := testutil.CurrenRowToMap(rows)
		if err != nil {
			return nil, err
		}
		for k, v := range rowmap {
			if v2, ok := v.([]byte); ok {
				rowmap[k] = string(v2)
			}
		}
		result = append(result, rowmap)
	}
	return result, nil
}

func (tctx *testContext) stepClusterEnvironmentIs(body *godog.DocString) error {
	byLine := func(r rune) bool {
		return r == '\n' || r == '\r'
	}
	for _, e := range strings.FieldsFunc(body.Content, byLine) {
		if e = strings.TrimSpace(e); e != "" {
			tctx.composerEnv = append(tctx.composerEnv, e)
		}
	}

	return nil
}

func (tctx *testContext) stepClusterIsUpAndRunning() error {
	err := tctx.composer.Up(tctx.composerEnv)
	if err != nil {
		return fmt.Errorf("failed to setup compose cluster: %s", err)
	}

	if _, ok := tctx.userDbs[shardUser]; !ok {
		tctx.userDbs[shardUser] = make(map[string]*sql.DB)
	}

	// check databases
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrShardName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get shard addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, shardUser, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to postgresql %s: %s", service, err)
			}
			tctx.userDbs[shardUser][service] = db
		}
	}

	// check router
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrRouterName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, shardUser, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.userDbs[shardUser][service] = db

			// router console
			addr, err = tctx.composer.GetAddr(service, spqrConsolePort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err = tctx.connectRouterConsoleWithCredentials(shardUser, shardPassword, addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.userDbs[shardUser][fmt.Sprintf("%s-admin", service)] = db
		}
	}

	// check coordinator
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrCoordinatorName) {
			addr, err := tctx.composer.GetAddr(service, spqrCoordinatorPort)
			if err != nil {
				return fmt.Errorf("failed to get coordinator addr %s: %s", service, err)
			}
			db, err := tctx.connectCoordinatorWithCredentials(shardUser, coordinatorPassword, addr, postgresqlInitialConnectTimeout)
			if err != nil {
				log.Printf("failed to connect to SPQR coordinator %s: %s", service, err)
				continue
			}
			tctx.userDbs[shardUser][service] = db
		}
	}

	// check qdb
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrQDBName) {
			addr, err := tctx.composer.GetAddr(service, qdbPort)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
			}

			db, err := qdb.NewEtcdQDB(addr, 0)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
			}
			tctx.qdb = db
		}
	}

	return nil
}

func (tctx *testContext) stepHostIsStopped(service string) error {
	log.Printf("begin stop %s", service)
	for _, dbs := range tctx.userDbs {
		if db, ok := dbs[service]; ok {
			if err := db.Close(); err != nil {
				return err
			}
			delete(dbs, service)
		}
	}

	err := tctx.composer.Stop(service)
	if err != nil {
		return fmt.Errorf("failed to stop service %s: %s", service, err)
	}

	//wait for the container to change state from "running".  stop operation and changing state is async
	checkNotRunningService := func() bool {
		if state, err := tctx.composer.ContainerState(service); err != nil {
			return false
		} else {
			return state != SERVICE_STATE_RUNNING
		}
	}
	retryStop := testutil.Retry(checkNotRunningService, time.Minute, time.Second)
	if !retryStop {
		return fmt.Errorf("timed out change state from 'running' %s", service)
	}

	if service == spqrQdbHost {
		log.Printf("it's QDB (%s). coordinator can't answer when it's stopped", service)
		return nil
	}
	anyCoord := false
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrCoordinatorName) {
			state, err := tctx.composer.ContainerState(service)
			if err != nil {
				return err
			}
			if state == SERVICE_STATE_RUNNING {
				anyCoord = true
				break
			}
		}
	}
	if !anyCoord {
		log.Printf("all coordinators are stopped, skip QDB check\n")
		return nil
	}
	if service == spqrQdbHost {
		log.Printf("it's QDB (%s). coordinator can't answer when it's stopped\n", service)
		return nil
	}
	// need to make sure another coordinator took control
	retryRes := testutil.Retry(tctx.checkCoordinatorInQDBFunc(""), time.Minute, time.Second)
	if !retryRes {
		return fmt.Errorf("timed out waiting for another coordinator to take control after stopping %s", service)
	}
	log.Printf("end stop %s", service)
	return nil
}

func (tctx *testContext) stepHostIsStarted(service string) error {
	err := tctx.composer.Start(service)
	if err != nil {
		return fmt.Errorf("failed to start service %s: %s", service, err)
	}

	if _, ok := tctx.userDbs[shardUser]; !ok {
		tctx.userDbs[shardUser] = make(map[string]*sql.DB)
	}

	// check databases
	if strings.HasPrefix(service, spqrShardName) {
		addr, err := tctx.composer.GetAddr(service, spqrPort)
		if err != nil {
			return fmt.Errorf("failed to get shard addr %s: %s", service, err)
		}
		db, err := tctx.connectPostgresql(addr, shardUser, postgresqlInitialConnectTimeout)
		if err != nil {
			return fmt.Errorf("failed to connect to postgresql %s: %s", service, err)
		}
		tctx.userDbs[shardUser][service] = db
		return nil
	}

	// check router
	if strings.HasPrefix(service, spqrRouterName) {
		addr, err := tctx.composer.GetAddr(service, spqrPort)
		if err != nil {
			return fmt.Errorf("failed to get router addr %s: %s", service, err)
		}
		db, err := tctx.connectPostgresql(addr, shardUser, 10*postgresqlInitialConnectTimeout)
		if err != nil {
			return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
		}
		tctx.userDbs[shardUser][service] = db

		// router console
		addr, err = tctx.composer.GetAddr(service, spqrConsolePort)
		if err != nil {
			return fmt.Errorf("failed to get router addr %s: %s", service, err)
		}
		db, err = tctx.connectRouterConsoleWithCredentials(shardUser, shardPassword, addr, postgresqlInitialConnectTimeout)
		if err != nil {
			return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
		}
		service = fmt.Sprintf("%s-admin", service)
		tctx.userDbs[shardUser][service] = db

		return nil
	}

	// check coordinator
	if strings.HasPrefix(service, spqrCoordinatorName) {
		addr, err := tctx.composer.GetAddr(service, spqrCoordinatorPort)
		if err != nil {
			return fmt.Errorf("failed to get router addr %s: %s", service, err)
		}
		db, err := tctx.connectCoordinatorWithCredentials(shardUser, coordinatorPassword, addr, postgresqlInitialConnectTimeout)
		if err != nil {
			log.Printf("failed to connect to SPQR coordinator %s: %s", service, err)
		} else {
			tctx.userDbs[shardUser][service] = db
		}
		return nil
	}

	// check qdb
	if strings.HasPrefix(service, spqrQDBName) {
		addr, err := tctx.composer.GetAddr(service, qdbPort)
		if err != nil {
			return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
		}

		db, err := qdb.NewEtcdQDB(addr, 0)
		if err != nil {
			return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
		}
		tctx.qdb = db

		log.Println("wait for QDB be ready")
		retryRes := testutil.Retry(func() bool {
			db, err := qdb.NewEtcdQDB(addr, 0)
			if err != nil {
				return false
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = db.ListShards(ctx)
			return err == nil
		}, time.Minute, time.Second)
		if !retryRes {
			return fmt.Errorf("SPQR QDB %s isn't ready after 1 minute", service)
		}

		return nil
	}

	return fmt.Errorf("service %s was not found in docker composer", service)
}

func (tctx *testContext) stepWaitPostgresqlToRespond(host string) error {
	const trials = 10
	const timeout = 20 * time.Second
	for range trials {
		_, err := tctx.queryPostgresql(host, shardUser, "SELECT 1", postgresqlQueryTimeout, make([]any, 0))
		if err == nil {
			return nil
		}
		time.Sleep(timeout)
	}
	return fmt.Errorf("host \"%s\" did not respond until timeout", host)
}

func (tctx *testContext) stepIRunCommandOnHost(host string, body *godog.DocString) error {
	cmd := strings.TrimSpace(body.Content)
	var err error
	tctx.commandRetcode, tctx.commandOutput, err = tctx.composer.RunCommand(host, cmd, commandExecutionTimeout)
	return err
}

func (tctx *testContext) stepIRunCommandsOnHost(host string, body *godog.DocString) error {
	commands := strings.Split(strings.TrimSpace(body.Content), "\n")
	var lastOutput string
	var lastRetCode int
	for _, command := range commands {
		cmd := strings.TrimSpace(command)
		var err error
		lastRetCode, lastOutput, err = tctx.composer.RunCommand(host, cmd, commandExecutionTimeout)
		if lastRetCode != 0 {
			log.Println("Get non zero code from command")
			log.Println(cmd)
			log.Println(lastRetCode)
			log.Println(lastOutput)
		}
		if err != nil {
			tctx.commandRetcode = lastRetCode
			tctx.commandOutput = lastOutput
			return err
		}
	}
	tctx.commandRetcode = lastRetCode
	tctx.commandOutput = lastOutput
	return nil
}

func (tctx *testContext) stepIRunCommandOnHostWithTimeout(host string, timeout int, body *godog.DocString) error {
	cmd := strings.TrimSpace(body.Content)
	var err error
	tctx.commandRetcode, tctx.commandOutput, err = tctx.composer.RunCommand(host, cmd, time.Duration(timeout)*time.Second)
	return err
}

func (tctx *testContext) stepCommandReturnCodeShouldBe(code int) error {
	if tctx.commandRetcode != code {
		return fmt.Errorf("command return code is %d, while expected %d\n%s", tctx.commandRetcode, code, tctx.commandOutput)
	}
	return nil
}

func (tctx *testContext) stepCommandOutputShouldMatch(matcher string, body *godog.DocString) error {
	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}
	return m(tctx.commandOutput, strings.TrimSpace(body.Content))
}

func (tctx *testContext) stepIRunSQLOnHost(host string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	_, err := tctx.queryPostgresql(host, shardUser, query, postgresqlQueryTimeout, make([]any, 0))
	return err
}

func (tctx *testContext) stepIRunSQLOnHostWithTimeout(host string, timeout int, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	_, err := tctx.queryPostgresql(host, shardUser, query, time.Duration(timeout)*time.Second, make([]any, 0))
	return err
}

func (tctx *testContext) stepIRunSQLOnHostAsUser(host string, user string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	_, err := tctx.queryPostgresql(host, user, query, postgresqlQueryTimeout, make([]any, 0))
	return err
}

func (tctx *testContext) stepIPrepareSQLOnHost(host string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	err := tctx.prepareQueryPostgresql(host, shardUser, query)
	return err
}

func (tctx *testContext) stepIRunPreparedSQLOnHost(host string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	_, err := tctx.queryPreparedPostgresql(host, query, make([]any, 0))
	return err
}

func (tctx *testContext) stepSQLResultShouldNotMatch(matcher string, body *godog.DocString) error {
	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}
	res, err := json.Marshal(tctx.sqlQueryResult)
	if err != nil {
		panic(err)
	}
	err = m(string(res), strings.TrimSpace(body.Content))

	if err != nil {
		return nil
	}
	return fmt.Errorf("should not match")
}

func (tctx *testContext) stepSQLResultShouldMatch(matcher string, body *godog.DocString) error {
	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}
	res, err := json.Marshal(tctx.sqlQueryResult)
	if err != nil {
		panic(err)
	}
	return m(string(res), strings.TrimSpace(body.Content))
}

func (tctx *testContext) stepIExecuteSql(host string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	err := tctx.executePostgresql(host, query)
	return err
}

func (tctx *testContext) stepFileOnHostShouldMatch(path string, node string, matcher string, body *godog.DocString) (err error) {
	remoteFile, err := tctx.composer.GetFile(node, path)
	if err != nil {
		return err
	}
	defer func() { _ = remoteFile.Close() }()

	var res strings.Builder
	for {
		buf := make([]byte, 4096)
		n, err := remoteFile.Read(buf)
		res.WriteString(string(buf[:n]))
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	actualContent := res.String()
	expectedContent := strings.TrimSpace(body.Content)

	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}
	return m(actualContent, expectedContent)
}

func (tctx *testContext) stepSaveResponseBodyAtPathAsJSON(rowIndex string, column string) error {
	i, err := strconv.Atoi(rowIndex)
	if err != nil {
		return fmt.Errorf("failed to get row index: %q not a number", rowIndex)
	}
	if i >= len(tctx.sqlQueryResult) {
		return fmt.Errorf("failed to get row at index %d: index is out of range", i)
	}

	a, b := tctx.sqlQueryResult[i][column]
	if !b {
		return fmt.Errorf("column does not exist")
	}

	actualPartByte, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("can't marshal to json: %w", err)
	}

	tctx.variables[column] = string(actualPartByte)
	return nil
}

func (tctx *testContext) stepHideField(fieldName string) error {
	for _, row := range tctx.sqlQueryResult {
		_, ok := row[fieldName]
		if ok {
			row[fieldName] = "**IGNORE**"
		}
	}
	return nil
}

func (tctx *testContext) stepRecordQDBTx(key string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var st qdb.DataTransferTransaction
	if err := json.Unmarshal([]byte(query), &st); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal request")
		return err
	}

	return tctx.qdb.RecordTransferTx(context.TODO(), key, &st)
}

func (tctx *testContext) stepQDBShouldNotContainRelation(key string) error {
	qname := rfqn.RelationFQN{RelationName: key}
	_, err := tctx.qdb.GetRelationDistribution(context.TODO(), &qname)
	switch t := err.(type) {
	case *spqrerror.SpqrError:
		if t.ErrorCode == spqrerror.SPQR_OBJECT_NOT_EXIST {
			return nil
		}
		return err
	case nil:
		return fmt.Errorf("relation \"%s\" is still attached in qdb", key)
	default:
		return err
	}
}

func (tctx *testContext) stepRecordQDBKRMove(body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var m qdb.MoveKeyRange
	if err := json.Unmarshal([]byte(query), &m); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal request")
		return err
	}

	return tctx.qdb.RecordKeyRangeMove(context.TODO(), &m)
}

func (tctx *testContext) stepRecordQDBTaskGroup(body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var taskGroup tasks.MoveTaskGroup
	if err := json.Unmarshal([]byte(query), &taskGroup); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal request")
		return err
	}

	taskDb := tasks.MoveTaskToDb(taskGroup.CurrentTask)

	return tctx.qdb.WriteMoveTaskGroup(context.TODO(), taskGroup.ID, tasks.TaskGroupToDb(&taskGroup), taskGroup.TotalKeys, taskDb)
}

func (tctx *testContext) stepRecordQDBRedistributeTask(body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var task *tasks.RedistributeTask
	if err := json.Unmarshal([]byte(query), &task); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal request")
		return err
	}

	return tctx.qdb.CreateRedistributeTask(context.TODO(), tasks.RedistributeTaskToDB(task))
}

func (tctx *testContext) stepRecordQDBTaskGroupStatus(id string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var status *qdb.TaskGroupStatus
	if err := json.Unmarshal([]byte(query), &status); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to unmarshal request")
		return err
	}

	return tctx.qdb.WriteTaskGroupStatus(context.TODO(), id, status)
}

func (tctx *testContext) stepQDBShouldContainTx(key string) error {
	tx, err := tctx.qdb.GetTransferTx(context.TODO(), key)
	if err != nil {
		return err
	}

	if tx == nil || tx.Status == "" {
		return fmt.Errorf("no valid transaction with key %s", key)
	}
	return nil
}

func (tctx *testContext) stepQDBShouldNotContainTx(key string) error {
	tx, err := tctx.qdb.GetTransferTx(context.TODO(), key)
	if tx == nil || err != nil || tx.Status == "" {
		return nil
	}
	return fmt.Errorf("valid transaction present with key %s", key)
}

func (tctx *testContext) stepQDBShouldNotContainKRMoves() error {
	txs, err := tctx.qdb.ListKeyRangeMoves(context.TODO())
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return nil
	}
	for _, v := range txs {
		log.Printf("txs '%#v'", v)
	}
	return fmt.Errorf("key range moves present")
}

func (tctx *testContext) stepErrorShouldMatch(host string, matcher string, body *godog.DocString) error {
	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}
	sqlErr, ok := tctx.sqlUserQueryError.Load(host)
	if !ok {
		return fmt.Errorf("host %s didn't get any error", host)
	}
	tctx.sqlUserQueryError.Delete(host)
	res, err := json.Marshal(sqlErr)
	if err != nil {
		panic(err)
	}
	return m(string(res), strings.TrimSpace(body.Content))
}

func (tctx *testContext) checkCoordinatorInQDBFunc(expected string) func() bool {
	return func() bool {
		_, output, err := tctx.composer.RunCommandJSON(spqrQdbHost, []string{"/usr/local/bin/etcdctl", "get", "coordinator_exists"}, time.Second)
		if err != nil {
			return false
		}
		if output == "" {
			return false
		}
		log.Printf("ETCD key:coordinator_exists %#v\n", output)
		addr := strings.Split(output, "\n")[1]
		addrWithoutPort := strings.Split(addr, ":")[0]
		if expected != "" && addrWithoutPort != expected {
			return false
		}
		port := strings.Split(addr, ":")[1]
		if mappedPort, err := tctx.composer.GetMappedPort(addrWithoutPort, port); err != nil {
			log.Printf("cant't get real port: %s\n", err.Error())
			return false
		} else {
			addr = fmt.Sprintf("%s:%d", "localhost", mappedPort)
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return false
		}
		defer func() {
			_ = conn.Close()
		}()

		client := protos.NewRouterServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), checkCoordinatorTimeout)
		defer cancel()
		log.Printf("try send command ListRouters to %s\n", addr)
		if _, err = client.ListRouters(ctx, nil); err == nil {
			log.Printf("listed routers from %s: successfully", addr)
		} else {
			log.Printf("can't list routers. error: %s", err.Error())
		}
		return err == nil
	}
}

func (tctx *testContext) stepCoordinatorShouldTakeControl(leader string) error {
	retryRes := testutil.Retry(tctx.checkCoordinatorInQDBFunc(leader), time.Minute, time.Second)
	if !retryRes {
		return fmt.Errorf("timed out waiting for \"%s\" to take control", leader)
	}
	return nil
}

func (tctx *testContext) stepWaitForCoordinatorAddressToBe(host string, leader string) error {
	retryRes := testutil.Retry(
		func() bool {
			res, err := tctx.queryPostgresql(host, shardUser, "SHOW "+spqrparser.CoordinatorAddrStr, postgresqlQueryTimeout, make([]any, 0))
			if err != nil {
				log.Printf("error waiting for coordinator address: %s", err)
				return false
			}
			if len(res) == 0 {
				return false
			}
			v, ok := res[0][spqrparser.CoordinatorAddrStr]
			if !ok {
				log.Printf("got incorrect result waiting for coordinator address: %s", res)
			}
			actualLeader, ok := v.(string)
			if !ok {
				log.Printf("got incorrect result waiting for coordinator address: %s", res)
				return false
			}
			return actualLeader == leader
		},
		time.Minute,
		time.Second,
	)
	if !retryRes {
		return fmt.Errorf("timed out waiting for \"%s\"'s coordinator address to be updated", host)
	}
	return nil
}

func (tctx *testContext) stepWaitForAllKeyRangeMovesToFinish(timeout int64) error {
	const interval = time.Second
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(timeout)*time.Second)
	defer cancel()
	return retry.Do(ctx, retry.NewConstant(interval), func(ctx context.Context) error {
		redistributeTasks, err := tctx.qdb.ListRedistributeTasks(ctx)
		if err != nil {
			log.Printf("error getting redistribute task: %s", err)
			return err
		}
		if len(redistributeTasks) > 0 {
			log.Println("redistribute task(s) present in qdb")
			return retry.RetryableError(fmt.Errorf("redistribute task(s) still present"))
		}
		taskGroups, err := tctx.qdb.ListTaskGroups(ctx)
		if err != nil {
			log.Printf("error getting move task group: %s", err)
			return err
		}
		if len(taskGroups) > 0 {
			log.Println("move task group present in qdb")
			return retry.RetryableError(fmt.Errorf("move task group still present"))
		}
		moveTasks, err := tctx.qdb.ListMoveTasks(ctx)
		if err != nil {
			log.Printf("error getting move task: %s", err)
			return err
		}
		if len(moveTasks) > 0 {
			log.Printf("move tasks present in qdb\n")
			return retry.RetryableError(fmt.Errorf("move task still present"))
		}
		krMoves, err := tctx.qdb.ListKeyRangeMoves(ctx)
		if err != nil {
			log.Printf("error getting key range moves: %s", err)
			return err
		}
		if len(krMoves) > 0 {
			log.Printf("%d key range moves still present in qbd", len(krMoves))
			return retry.RetryableError(fmt.Errorf("key range moves still present"))
		}
		return nil
	})
}

func (tctx *testContext) stepQDBShouldNotContainTasks() error {
	ctx, cancel := context.WithTimeout(context.TODO(), qdbQueriesTimeout)
	defer cancel()
	redistributeTasks, err := tctx.qdb.ListRedistributeTasks(ctx)
	if err != nil {
		log.Printf("error getting redistribute task: %s", err)
		return err
	}
	if len(redistributeTasks) > 0 {
		log.Println("redistribute task(s) present in qdb")
		return retry.RetryableError(fmt.Errorf("redistribute task(s) still present"))
	}
	taskGroups, err := tctx.qdb.ListTaskGroups(ctx)
	if err != nil {
		log.Printf("error getting move task group: %s", err)
		return err
	}
	if len(taskGroups) > 0 {
		log.Println("move task group present in qdb")
		return retry.RetryableError(fmt.Errorf("move task group still present"))
	}
	moveTasks, err := tctx.qdb.ListMoveTasks(ctx)
	if err != nil {
		log.Printf("error getting move task: %s", err)
		return err
	}
	if len(moveTasks) > 0 {
		log.Printf("move tasks present in qdb\n")
		return retry.RetryableError(fmt.Errorf("move task still present"))
	}
	krMoves, err := tctx.qdb.ListKeyRangeMoves(ctx)
	if err != nil {
		log.Printf("error getting key range moves: %s", err)
		return err
	}
	if len(krMoves) > 0 {
		log.Printf("%d key range moves still present in qbd", len(krMoves))
		return retry.RetryableError(fmt.Errorf("key range moves still present"))
	}
	return nil
}

func (tctx *testContext) stepIKillHostAfterQuery(host string, delay int, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	db, err := tctx.getPostgresqlConnection(shardUser, host)
	if err != nil {
		return err
	}
	go func() {
		_ = db.QueryRow(query)
	}()
	time.Sleep(time.Duration(delay) * time.Second)

	return tctx.stepHostIsStopped(host)
}

func InitializeScenario(s *godog.ScenarioContext, t *testing.T, debug bool) {
	tctx, err := newTestContext(t)
	if err != nil {
		t.Fatal(err)
	}
	tctx.debug = debug

	s.Before(func(ctx context.Context, scenario *godog.Scenario) (context.Context, error) {
		//tctx.cleanup()
		tctx.composerEnv = []string{
			"ROUTER_CONFIG=/spqr/test/feature/conf/router.yaml",
			"COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml",
			"ROUTER_COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml",
			"ROUTER_2_COORDINATOR_CONFIG=/spqr/test/feature/conf/coordinator.yaml",
		}
		tctx.variables = make(map[string]any)
		return ctx, nil
	})
	s.StepContext().Before(func(ctx context.Context, step *godog.Step) (context.Context, error) {
		tctx.templateErr = tctx.templateStep(step)
		return ctx, nil
	})
	s.StepContext().After(func(ctx context.Context, step *godog.Step, status godog.StepResultStatus, err error) (context.Context, error) {
		if err != nil {
			if !debug {
				return ctx, err
			}
			log.Println(err)
			log.Println("sleeping")
			time.Sleep(time.Hour)
		}
		if tctx.templateErr != nil {
			log.Printf("error in templating %s: %v\n", step.Text, tctx.templateErr)
		}
		return ctx, nil
	})
	s.After(func(ctx context.Context, scenario *godog.Scenario, err error) (context.Context, error) {
		if err != nil {
			name := scenario.Name
			name = strings.ReplaceAll(name, " ", "_")
			err2 := tctx.saveLogs(name)
			if err2 != nil {
				log.Printf("failed to save logs: %v", err2)
			}
			if v, _ := os.LookupEnv("GODOG_NO_CLEANUP"); v != "" {
				return ctx, nil
			}
		}
		tctx.cleanup()
		return ctx, nil
	})

	// host manipulation
	s.Step(`^cluster environment is$`, tctx.stepClusterEnvironmentIs)
	s.Step(`^cluster is up and running$`, tctx.stepClusterIsUpAndRunning)
	s.Step(`^cluster is failed up and running$`, func() error {
		err := tctx.stepClusterIsUpAndRunning()
		if err != nil {
			return nil
		}
		return fmt.Errorf("cluster is up")
	})
	s.Step(`^host "([^"]*)" is stopped$`, tctx.stepHostIsStopped)
	s.Step(`^host "([^"]*)" is started$`, tctx.stepHostIsStarted)

	// command and SQL execution
	s.Step(`^I run command on host "([^"]*)"$`, tctx.stepIRunCommandOnHost)
	s.Step(`^I run commands on host "([^"]*)"$`, tctx.stepIRunCommandsOnHost)
	s.Step(`^I run command on host "([^"]*)" with timeout "(\d+)" seconds$`, tctx.stepIRunCommandOnHostWithTimeout)
	s.Step(`^command return code should be "(\d+)"$`, tctx.stepCommandReturnCodeShouldBe)
	s.Step(`^command output should match (\w+)$`, tctx.stepCommandOutputShouldMatch)
	s.Step(`^I run SQL on host "([^"]*)"$`, tctx.stepIRunSQLOnHost)
	s.Step(`^I run SQL on host "([^"]*)" with timeout "(\d+)" seconds$`, tctx.stepIRunSQLOnHostWithTimeout)
	s.Step(`^I run SQL on host "([^"]*)" as user "([^"]*)"$`, tctx.stepIRunSQLOnHostAsUser)
	s.Step(`^I execute SQL on host "([^"]*)"$`, tctx.stepIExecuteSql)
	s.Step(`^I prepare SQL on host "([^"]*)"$`, tctx.stepIPrepareSQLOnHost)
	s.Step(`^I run prepared SQL on host "([^"]*)"$`, tctx.stepIRunPreparedSQLOnHost)
	s.Step(`^SQL result should match (\w+)$`, tctx.stepSQLResultShouldMatch)
	s.Step(`^we wait for "(\d+)" seconds$`, func(sleepFor int) error {

		time.Sleep(time.Duration(sleepFor) * time.Second)

		return nil
	})
	s.Step(`^I run SQL on host "([^"]*)" in parallel with timeout "(\d+)" seconds$`, tctx.stepIExecuteSqlInParallel)

	s.Step(`^SQL result should not match (\w+)$`, tctx.stepSQLResultShouldNotMatch)
	s.Step(`^I record in qdb data transfer transaction with name "([^"]*)"$`, tctx.stepRecordQDBTx)
	s.Step(`^qdb should not contain relation "([^"]*)"$`, tctx.stepQDBShouldNotContainRelation)
	s.Step(`^I record in qdb key range move$`, tctx.stepRecordQDBKRMove)
	s.Step(`^I record in qdb move task group$`, tctx.stepRecordQDBTaskGroup)
	s.Step(`^I record in qdb status of move task group "([^"]*)"$`, tctx.stepRecordQDBTaskGroupStatus)
	s.Step(`^I record in qdb redistribute task$`, tctx.stepRecordQDBRedistributeTask)
	s.Step(`^qdb should contain transaction "([^"]*)"$`, tctx.stepQDBShouldContainTx)
	s.Step(`^qdb should not contain transaction "([^"]*)"$`, tctx.stepQDBShouldNotContainTx)
	s.Step(`^qdb should not contain key range moves$`, tctx.stepQDBShouldNotContainKRMoves)
	s.Step(`^SQL error on host "([^"]*)" should match (\w+)$`, tctx.stepErrorShouldMatch)
	s.Step(`^file "([^"]*)" on host "([^"]*)" should match (\w+)$`, tctx.stepFileOnHostShouldMatch)
	s.Step(`^I wait for host "([^"]*)" to respond$`, tctx.stepWaitPostgresqlToRespond)
	s.Step(`^I wait for coordinator "([^"]*)" to take control$`, tctx.stepCoordinatorShouldTakeControl)
	s.Step(`^I wait for coordinator address on router "([^"]*)" to become "([^"]*)"$`, tctx.stepWaitForCoordinatorAddressToBe)
	s.Step(`^I wait for "(\d+)" seconds for all key range moves to finish$`, tctx.stepWaitForAllKeyRangeMovesToFinish)
	s.Step(`^qdb should not contain transfer tasks$`, tctx.stepQDBShouldNotContainTasks)
	s.Step(`^I run SQL on host "([^"]*)", then stop the host after "(\d+)" seconds$`, tctx.stepIKillHostAfterQuery)

	// variable manipulation
	s.Step(`^we save response row "([^"]*)" column "([^"]*)"$`, tctx.stepSaveResponseBodyAtPathAsJSON)
	s.Step(`^hide "([^"]*)" field$`, tctx.stepHideField)
}

func TestSpqr(t *testing.T) {
	paths := make([]string, 0)
	featureDir := "features"
	if featureDirEnv, ok := os.LookupEnv("GODOG_FEATURE_DIR"); ok {
		if len(featureDirEnv) != 0 {
			featureDir = featureDirEnv
		}
	}
	if featureEnv, ok := os.LookupEnv("GODOG_FEATURE"); ok {
		for feature := range strings.SplitSeq(featureEnv, ";") {
			if !strings.HasSuffix(feature, ".feature") {
				feature += ".feature"
			}
			paths = append(paths, fmt.Sprintf("%s/%s", featureDir, feature))
		}
	}
	if len(paths) == 0 {
		paths = append(paths, featureDir)
	}

	debug := false
	if debugEnv, ok := os.LookupEnv("FEATURE_DEBUG"); ok && strings.ToLower(debugEnv) == "true" {
		debug = true
	}

	suite := godog.TestSuite{
		ScenarioInitializer: func(s *godog.ScenarioContext) {
			InitializeScenario(s, t, debug)
		},
		Options: &godog.Options{
			Format:        "pretty",
			Paths:         paths,
			Strict:        true,
			NoColors:      false,
			StopOnFailure: true,
			Concurrency:   1,
		},
	}
	if suite.Run() != 0 {
		t.Fail()
	}
}
