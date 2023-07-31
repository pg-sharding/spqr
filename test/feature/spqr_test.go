package tests

import (
	"context"
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

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/test/feature/testutil"
	"github.com/pg-sharding/spqr/test/feature/testutil/matchers"
)

const (
	spqrShardName                   = "shard"
	spqrRouterName                  = "router"
	spqrCoordinatorName             = "coordinator"
	spqrQDBName                     = "qdb"
	spqrPort                        = 6432
	spqrConsolePort                 = 7432
	coordinatorPort                 = 7002
	qdbPort                         = 2379
	shardUser                       = "regress"
	shardPassword                   = ""
	dbName                          = "regress"
	postgresqlConnectTimeout        = 30 * time.Second
	postgresqlInitialConnectTimeout = 2 * time.Minute
	postgresqlQueryTimeout          = 2 * time.Second
)

type Addr struct {
	host string
	port int
}

type testContext struct {
	variables         map[string]interface{}
	templateErr       error
	composer          testutil.Composer
	composerEnv       []string
	dbs               map[Addr]*sqlx.DB
	sqlQueryResult    []map[string]interface{}
	sqlUserQueryError sync.Map // host -> error
	commandRetcode    int
	commandOutput     string
	qdb               qdb.QDB
	t                 *testing.T
}

func newTestContext(t *testing.T) (*testContext, error) {
	var err error
	tctx := new(testContext)
	tctx.composer, err = testutil.NewDockerComposer("spqr", "docker-compose.yaml")
	tctx.t = t
	if err != nil {
		return nil, err
	}
	tctx.dbs = make(map[Addr]*sqlx.DB)
	return tctx, nil
}

var postgresqlLogsToSave = map[string]string{
	"/var/log/postgresql/postgresql-13-main.log": "postgresql.log",
}

func (tctx *testContext) saveLogs(scenario string) error {
	var errs []error
	for _, service := range tctx.composer.Services() {
		var logsToSave map[string]string
		switch {
		case strings.HasPrefix(service, spqrShardName):
			logsToSave = postgresqlLogsToSave
		default:
			continue
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
		msg := ""
		for _, err := range errs {
			msg += err.Error() + "\n"
		}
		return errors.New(msg)
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
	for _, db := range tctx.dbs {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db connection: %s", err)
		}
	}
	tctx.dbs = make(map[Addr]*sqlx.DB)
	if err := tctx.composer.Down(); err != nil {
		log.Printf("failed to tear down compose: %s", err)
	}
	tctx.variables = make(map[string]interface{})
	tctx.composerEnv = make([]string, 0)
	tctx.sqlQueryResult = make([]map[string]interface{}, 0)
	tctx.sqlUserQueryError = sync.Map{}
	tctx.commandRetcode = 0
	tctx.commandOutput = ""
}

// nolint: unparam
func (tctx *testContext) connectPostgresql(addr string, timeout time.Duration) (*sqlx.DB, error) {
	if strings.Contains(addr, strconv.Itoa(coordinatorPort)) || strings.Contains(addr, strconv.Itoa(spqrConsolePort)) {
		return tctx.connectCoordinatorWithCredentials(shardUser, shardPassword, addr, timeout)
	}
	return tctx.connectPostgresqlWithCredentials(shardUser, shardPassword, addr, timeout)
}

func (tctx *testContext) connectPostgresqlWithCredentials(username string, password string, addr string, timeout time.Duration) (*sqlx.DB, error) {
	connTimeout := 2 * time.Second
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, addr, dbName)
	connCfg, _ := pgx.ParseConfig(dsn)
	connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connCfg.RuntimeParams["client_encoding"] = "UTF8"
	connStr := stdlib.RegisterConnConfig(connCfg)
	db, err := sqlx.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}
	// sql is lazy in go, so we need ping db
	testutil.Retry(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		err = db.PingContext(ctx)
		return err == nil
	}, timeout, connTimeout)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (tctx *testContext) connectCoordinatorWithCredentials(username string, password string, addr string, timeout time.Duration) (*sqlx.DB, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, addr, dbName)
	connCfg, _ := pgx.ParseConfig(dsn)
	connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connCfg.RuntimeParams["client_encoding"] = "UTF8"
	connStr := stdlib.RegisterConnConfig(connCfg)
	db, err := sqlx.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}
	// sql is lazy in go, so we need ping db
	testutil.Retry(func() bool {
		_, err = db.Exec("SHOW routers")
		return err == nil
	}, timeout, 2*time.Second)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (tctx *testContext) getPostgresqlConnection(dbAddr Addr) (*sqlx.DB, error) {
	db, ok := tctx.dbs[dbAddr]
	if !ok {
		return nil, fmt.Errorf("postgresql %s is not in cluster", dbAddr.host)
	}
	err := db.Ping()
	if err == nil {
		return db, nil
	}
	addr, err := tctx.composer.GetAddr(dbAddr.host, dbAddr.port)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgresql addr (%s:%d): %s", dbAddr.host, dbAddr.port, err)
	}
	db, err = tctx.connectPostgresql(addr, postgresqlConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgresql (%s:%d): %s", dbAddr.host, dbAddr.port, err)
	}
	tctx.dbs[dbAddr] = db
	return db, nil
}

func (tctx *testContext) queryPostgresql(dbAddr Addr, query string, args interface{}) ([]map[string]interface{}, error) {
	db, err := tctx.getPostgresqlConnection(dbAddr)
	if err != nil {
		return nil, err
	}

	// sqlx can't execute requests with semicolon
	// we will execute them in single connection
	queries := strings.Split(query, ";")
	var result []map[string]interface{}

	for _, q := range queries {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		tctx.sqlQueryResult = nil
		result, err = tctx.doPostgresqlQuery(db, q, args, postgresqlQueryTimeout)
		tctx.sqlQueryResult = result
	}

	return result, err
}

func (tctx *testContext) executePostgresql(dbAddr Addr, query string, args interface{}) error {
	db, err := tctx.getPostgresqlConnection(dbAddr)
	if err != nil {
		return err
	}

	// sqlx can't execute requests with semicolon
	// we will execute them in single connection
	queries := strings.Split(query, ";")

	for _, q := range queries {
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

func (tctx *testContext) doPostgresqlQuery(db *sqlx.DB, query string, args interface{}, timeout time.Duration) ([]map[string]interface{}, error) {
	if args == nil {
		args = struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := db.NamedQueryContext(ctx, query, args)
	if err != nil {
		log.Printf("query error %#v\n", err)
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		rowmap := make(map[string]interface{})
		err = rows.MapScan(rowmap)
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

func (tctx *testContext) stepClusterIsUpAndRunning(createHaNodes bool) error {
	err := tctx.composer.Up(tctx.composerEnv)
	if err != nil {
		return fmt.Errorf("failed to setup compose cluster: %s", err)
	}

	connCfg := pgx.ConnConfig{
		DefaultQueryExecMode: pgx.QueryExecModeSimpleProtocol,
	}
	stdlib.RegisterConnConfig(&connCfg)

	// check databases
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrShardName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get shard addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to postgresql %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: spqrPort}] = db
		}
	}

	// check router
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrRouterName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: spqrPort}] = db

			addr, err = tctx.composer.GetAddr(service, spqrConsolePort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err = tctx.connectCoordinatorWithCredentials(shardUser, shardPassword, addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: spqrConsolePort}] = db
		}
	}

	// check coordinator
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrCoordinatorName) {
			addr, err := tctx.composer.GetAddr(service, coordinatorPort)
			if err != nil {
				return fmt.Errorf("failed to get coordinator addr %s: %s", service, err)
			}

			db, err := tctx.connectCoordinatorWithCredentials(shardUser, shardPassword, addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR coordinator %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: coordinatorPort}] = db
		}
	}

	// check qdb
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrQDBName) {
			addr, err := tctx.composer.GetAddr(service, qdbPort)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
			}
			spqrlog.Zero.Error().Msg(addr)

			db, err := qdb.NewEtcdQDB(addr)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR QDB %s: %s", service, err)
			}
			tctx.qdb = db
		}
	}

	return nil
}

func (tctx *testContext) stepCommandReturnCodeShouldBe(code int) error {
	if tctx.commandRetcode != code {
		return fmt.Errorf("command return code is %d, while expected %d\n%s", tctx.commandRetcode, code, tctx.commandOutput)
	}
	return nil
}

func (tctx *testContext) getAddr(host string) Addr {
	switch host {
	case "spqr-console":
		return Addr{host: "router", port: spqrConsolePort}
	case "coordinator":
		return Addr{host: spqrCoordinatorName, port: spqrConsolePort}
	default:
		return Addr{host: host, port: spqrPort}
	}
}

func (tctx *testContext) stepIRunSQLOnHost(host string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)

	_, err := tctx.queryPostgresql(tctx.getAddr(host), query, struct{}{})
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
	return fmt.Errorf("Should not match")
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

	err := tctx.executePostgresql(tctx.getAddr(host), query, struct{}{})
	return err
}

func (tctx *testContext) stepRestartRouter() error {
	err := tctx.composer.Stop("router")
	if err != nil {
		return err
	}

	err = tctx.composer.Start("router")

	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrRouterName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: spqrPort}].Close()
			tctx.dbs[Addr{host: service, port: spqrPort}] = db

			addr, err = tctx.composer.GetAddr(service, spqrConsolePort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err = tctx.connectCoordinatorWithCredentials(shardUser, shardPassword, addr, postgresqlInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.dbs[Addr{host: service, port: spqrConsolePort}].Close()
			tctx.dbs[Addr{host: service, port: spqrConsolePort}] = db
		}
	}

	return err
}

func (tctx *testContext) stepHostIsStopped(host string) error {
	return tctx.composer.Stop(host)
}

func (tctx *testContext) stepHostIsStarted(host string) error {
	return tctx.composer.Start(host)
}

func (tctx *testContext) stepRecordQDBTx(key string, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	var st qdb.DataTransferTransaction
	if err := json.Unmarshal([]byte(query), &st); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("Failed to unmarshal request")
		return err
	}

	return tctx.qdb.RecordTransferTx(context.TODO(), key, &st)
}

// nolint: unused
func InitializeScenario(s *godog.ScenarioContext, t *testing.T) {
	tctx, err := newTestContext(t)
	if err != nil {
		// TODO: how to report errors in godog
		panic(err)
	}

	s.Before(func(ctx context.Context, scenario *godog.Scenario) (context.Context, error) {
		//tctx.cleanup()
		return ctx, nil
	})
	s.StepContext().Before(func(ctx context.Context, step *godog.Step) (context.Context, error) {
		tctx.templateErr = tctx.templateStep(step)
		return ctx, nil
	})
	s.StepContext().After(func(ctx context.Context, step *godog.Step, status godog.StepResultStatus, err error) (context.Context, error) {
		if tctx.templateErr != nil {
			log.Fatalf("Error in templating %s: %v\n", step.Text, tctx.templateErr)
		}
		return ctx, nil
	})
	s.After(func(ctx context.Context, scenario *godog.Scenario, err error) (context.Context, error) {
		if err != nil {
			name := scenario.Name
			name = strings.Replace(name, " ", "_", -1)
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
	s.Step(`^cluster is up and running$`, func() error { return tctx.stepClusterIsUpAndRunning(true) })
	s.Step(`^host "([^"]*)" is stopped$`, tctx.stepHostIsStopped)
	s.Step(`^host "([^"]*)" is started$`, tctx.stepHostIsStarted)

	// command and SQL execution
	s.Step(`^command return code should be "(\d+)"$`, tctx.stepCommandReturnCodeShouldBe)
	s.Step(`^I restart router$`, tctx.stepRestartRouter)
	s.Step(`^I run SQL on host "([^"]*)"$`, tctx.stepIRunSQLOnHost)
	s.Step(`^I execute SQL on host "([^"]*)"$`, tctx.stepIExecuteSql)
	s.Step(`^SQL result should match (\w+)$`, tctx.stepSQLResultShouldMatch)
	s.Step(`^SQL result should not match (\w+)$`, tctx.stepSQLResultShouldNotMatch)
	s.Step(`^I record in qdb data transfer transaction with name "([^"]*)"$`, tctx.stepRecordQDBTx)

}

func TestSpqr(t *testing.T) {
	features := "features"
	if feauterEnv, ok := os.LookupEnv("GODOG_FEATURE"); ok {
		if !strings.HasSuffix(feauterEnv, ".feature") {
			feauterEnv += ".feature"
		}
		features = fmt.Sprintf("features/%s", feauterEnv)
	}
	suite := godog.TestSuite{
		ScenarioInitializer: func(s *godog.ScenarioContext) {
			InitializeScenario(s, t)
		},
		Options: &godog.Options{
			Format:        "pretty",
			Paths:         []string{features},
			Strict:        true,
			NoColors:      true,
			StopOnFailure: true,
			Concurrency:   1,
		},
	}
	if suite.Run() != 0 {
		t.Fail()
	}
}
