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
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/cucumber/godog"
	"github.com/go-sql-driver/mysql"
	"github.com/go-zookeeper/zk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/pg-sharding/spqr/test/featuretests/testutil"
	"github.com/pg-sharding/spqr/test/featuretests/testutil/matchers"
)

const (
	yes                        = "Yes"
	zkName                     = "zoo"
	zkPort                     = 2181
	zkConnectTimeout           = 5 * time.Second
	commandExecutionTimeout    = 10 * time.Second
	spqrShardName              = "shard"
	spqrRouterName             = "router"
	spqrCoordinatorName        = "coordinator"
	mysqlName                  = "mysql"
	mysqlPort                  = 3306
	spqrPort                   = 6432
	coordinatorPort            = 7002
	shardUser                  = "regress"
	shardPassword              = ""
	dbName                     = "regress"
	mysqlAdminUser             = "admin"
	mysqlAdminPassword         = "admin_pwd"
	mysqlOrdinaryUser          = "user"
	mysqlOrdinaryPassword      = "user_pwd"
	mysqlConnectTimeout        = 30 * time.Second
	spqrInitialConnectTimeout  = 2 * time.Minute
	mysqlQueryTimeout          = 2 * time.Second
	mysqlWaitOnlineTimeout     = 60
	replicationChannel         = ""
	ExternalReplicationChannel = "external"
	testUser                   = "testuser"
	testPassword               = "testpassword123"
)

var mysqlLogsToSave = map[string]string{
	"/var/log/supervisor.log":  "supervisor.log",
	"/var/log/mysync.log":      "mysync.log",
	"/var/log/mysql/error.log": "mysqld.log",
	"/var/log/mysql/query.log": "query.log",
	"/etc/mysync.yaml":         "mysync.yaml",
}

var zkLogsToSave = map[string]string{
	"/var/log/zookeeper/zookeeper--server-%s.log": "zookeeper.log",
}

type noLogger struct{}

func (noLogger) Printf(string, ...interface{}) {}

func (noLogger) Print(...interface{}) {}

type testContext struct {
	variables         map[string]interface{}
	templateErr       error
	composer          testutil.Composer
	composerEnv       []string
	zk                *zk.Conn
	dbs               map[string]*sqlx.DB
	zkQueryResult     string
	sqlQueryResult    []map[string]interface{}
	sqlUserQueryError sync.Map // host -> error
	commandRetcode    int
	commandOutput     string
	acl               []zk.ACL
}

func newTestContext() (*testContext, error) {
	var err error
	tctx := new(testContext)
	tctx.composer, err = testutil.NewDockerComposer("mysync", "images/docker-compose.yaml")
	if err != nil {
		return nil, err
	}
	tctx.dbs = make(map[string]*sqlx.DB)
	tctx.acl = zk.DigestACL(zk.PermAll, testUser, testPassword)
	return tctx, nil
}

func (tctx *testContext) saveLogs(scenario string) error {
	var errs []error
	for _, service := range tctx.composer.Services() {
		var logsToSave map[string]string
		switch {
		case strings.HasPrefix(service, mysqlName):
			logsToSave = mysqlLogsToSave
		case strings.HasPrefix(service, zkName):
			logsToSave = zkLogsToSave
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
	if tctx.zk != nil {
		tctx.zk.Close()
		tctx.zk = nil
	}
	for _, db := range tctx.dbs {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db connection: %s", err)
		}
	}
	tctx.dbs = make(map[string]*sqlx.DB)
	if err := tctx.composer.Down(); err != nil {
		log.Printf("failed to tear down compose: %s", err)
	}

	tctx.variables = make(map[string]interface{})
	tctx.composerEnv = make([]string, 0)
	tctx.zkQueryResult = ""
	tctx.sqlQueryResult = make([]map[string]interface{}, 0)
	tctx.sqlUserQueryError = sync.Map{}
	tctx.commandRetcode = 0
	tctx.commandOutput = ""
}

// nolint: unparam
func (tctx *testContext) connectMysql(addr string, timeout time.Duration) (*sqlx.DB, error) {
	return tctx.connectMysqlWithCredentials(mysqlAdminUser, mysqlAdminPassword, addr, timeout)
}

func (tctx *testContext) connectMysqlWithCredentials(username string, password string, addr string, timeout time.Duration) (*sqlx.DB, error) {
	_ = mysql.SetLogger(noLogger{})
	connTimeout := 2 * time.Second
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/mysql?timeout=%s", username, password, addr, connTimeout)
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// sql is lazy in go, so we need ping db
	testutil.Retry(func() bool {
		//var rows *sql.Rows
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

// nolint: unparam
func (tctx *testContext) connectPostgresql(addr string, timeout time.Duration) (*sqlx.DB, error) {
	return tctx.connectPostgresqlWithCredentials(shardUser, shardPassword, addr, timeout)
}

func (tctx *testContext) connectPostgresqlWithCredentials(username string, password string, addr string, timeout time.Duration) (*sqlx.DB, error) {
	connTimeout := 2 * time.Second
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", username, password, addr, dbName)
	db, err := sqlx.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	// sql is lazy in go, so we need ping db
	testutil.Retry(func() bool {
		//var rows *sql.Rows
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

func (tctx *testContext) getMysqlConnection(host string) (*sqlx.DB, error) {
	db, ok := tctx.dbs[host]
	if !ok {
		return nil, fmt.Errorf("mysql %s is not in cluster", host)
	}
	err := db.Ping()
	if err == nil {
		return db, nil
	}
	addr, err := tctx.composer.GetAddr(host, mysqlPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get mysql addr %s: %s", host, err)
	}
	db, err = tctx.connectMysql(addr, mysqlConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql %s: %s", host, err)
	}
	tctx.dbs[host] = db
	return db, nil
}

func (tctx *testContext) getPostgresqlConnection(host string) (*sqlx.DB, error) {
	db, ok := tctx.dbs[host]
	if !ok {
		return nil, fmt.Errorf("postgresql %s is not in cluster", host)
	}
	err := db.Ping()
	if err == nil {
		return db, nil
	}
	addr, err := tctx.composer.GetAddr(host, spqrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgresql addr %s: %s", host, err)
	}
	db, err = tctx.connectPostgresql(addr, mysqlConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgresql %s: %s", host, err)
	}
	tctx.dbs[host] = db
	return db, nil
}

func (tctx *testContext) queryMysql(host string, query string, args interface{}) ([]map[string]interface{}, error) {
	if args == nil {
		args = struct{}{}
	}
	db, err := tctx.getMysqlConnection(host)
	if err != nil {
		return nil, err
	}

	// sqlx can't execute requests with semicolon
	// we will execute them in single connection
	queries := strings.Split(query, ";")
	var result []map[string]interface{}

	for _, q := range queries {
		tctx.sqlQueryResult = nil
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}

		result, err = tctx.doMysqlQuery(db, q, args, mysqlQueryTimeout)
		tctx.sqlQueryResult = result
	}

	return result, err
}

func (tctx *testContext) queryPostgresql(host string, query string, args interface{}) ([]map[string]interface{}, error) {
	if args == nil {
		args = struct{}{}
	}
	db, err := tctx.getPostgresqlConnection(host)
	if err != nil {
		return nil, err
	}

	// sqlx can't execute requests with semicolon
	// we will execute them in single connection
	queries := strings.Split(query, ";")
	var result []map[string]interface{}

	for _, q := range queries {
		tctx.sqlQueryResult = nil
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}

		result, err = tctx.doPostgresqlQuery(db, q, args, mysqlQueryTimeout)
		tctx.sqlQueryResult = result
	}

	return result, err
}

func (tctx *testContext) doMysqlQuery(db *sqlx.DB, query string, args interface{}, timeout time.Duration) ([]map[string]interface{}, error) {
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

func (tctx *testContext) doPostgresqlQuery(db *sqlx.DB, query string, args interface{}, timeout time.Duration) ([]map[string]interface{}, error) {
	if args == nil {
		args = struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rows, err := db.NamedQueryContext(ctx, query, args)
	log.Printf("Query %s, result %#v\n", query, rows)
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
	log.Printf("Result %#v\n", result)
	return result, nil
}

func (tctx *testContext) stepClusterEnvironmentIs(body *godog.DocString) error {
	var env []string
	byLine := func(r rune) bool {
		return r == '\n' || r == '\r'
	}
	for _, e := range strings.FieldsFunc(body.Content, byLine) {
		if e = strings.TrimSpace(e); e != "" {
			env = append(env, e)
		}
	}
	version, _ := os.LookupEnv("VERSION")
	if version != "" {
		v := fmt.Sprintf("VERSION=%s", version)
		env = append(env, v)
	}
	tctx.composerEnv = env
	return nil
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
			db, err := tctx.connectPostgresql(addr, spqrInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to postgresql %s: %s", service, err)
			}
			tctx.dbs[service] = db
		}
	}

	// check router
	for _, service := range tctx.composer.Services() {
		if strings.HasPrefix(service, spqrRouterName) {
			addr, err := tctx.composer.GetAddr(service, spqrPort)
			if err != nil {
				return fmt.Errorf("failed to get router addr %s: %s", service, err)
			}
			db, err := tctx.connectPostgresql(addr, spqrInitialConnectTimeout)
			if err != nil {
				return fmt.Errorf("failed to connect to SPQR router %s: %s", service, err)
			}
			tctx.dbs[service] = db
		}
	}

	return nil
}

func (tctx *testContext) stepHostIsStopped(host string) error {
	return tctx.composer.Stop(host)
}

func (tctx *testContext) stepHostIsDetachedFromTheNetwork(host string) error {
	return tctx.composer.DetachFromNet(host)
}

func (tctx *testContext) stepHostIsStarted(host string) error {
	return tctx.composer.Start(host)
}

func (tctx *testContext) stepHostIsAttachedToTheNetwork(host string) error {
	return tctx.composer.AttachToNet(host)
}

func (tctx *testContext) stepHostShouldHaveFile(node string, path string) error {
	remoteFile, err := tctx.composer.GetFile(node, path)
	if err != nil {
		return err
	}
	err = remoteFile.Close()
	if err != nil {
		return err
	}
	return nil
}

func (tctx *testContext) stepHostShouldHaveNoFile(node string, path string) error {
	res, err := tctx.composer.CheckIfFileExist(node, path)
	if err != nil {
		return err
	}
	if res {
		return fmt.Errorf("file %s exists on %s", path, node)
	}
	return nil
}

func (tctx *testContext) stepHostShouldHaveFileWithin(node string, path string, timeout int) error {
	var err error
	testutil.Retry(func() bool {
		err = tctx.stepHostShouldHaveFile(node, path)
		return err == nil
	}, time.Duration(timeout*int(time.Second)), time.Second)
	return err
}

func (tctx *testContext) stepFileOnHostHaveContentOf(path string, node string, body *godog.DocString) error {
	remoteFile, err := tctx.composer.GetFile(node, path)
	if err != nil {
		return err
	}
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
	if actualContent != expectedContent {
		return fmt.Errorf("file %s on %s should contents %s but actually has %s", path, node, expectedContent, actualContent)
	}
	err = remoteFile.Close()
	return err
}

func (tctx *testContext) stepIRunCommandOnHost(host string, body *godog.DocString) error {
	cmd := strings.TrimSpace(body.Content)
	var err error
	tctx.commandRetcode, tctx.commandOutput, err = tctx.composer.RunCommand(host, cmd, commandExecutionTimeout)
	return err
}

func (tctx *testContext) stepSetUsedSpace(host string, percent int) error {
	if percent < 0 || percent > 100 {
		return fmt.Errorf("incorrect percent value: %d", percent)
	}
	cmd := fmt.Sprintf("rm /tmp/usedspace && echo %d > /tmp/usedspace", percent)
	_, _, err := tctx.composer.RunCommand(host, cmd, commandExecutionTimeout)
	return err
}

func (tctx *testContext) stepSetReadonlyStatus(host string, value string) error {
	if value != "true" && value != "false" {
		return fmt.Errorf("value must be true or false: %s", value)
	}
	cmd := fmt.Sprintf("rm /tmp/readonly && echo %s > /tmp/readonly", value)
	code, output, err := tctx.composer.RunCommand(host, cmd, commandExecutionTimeout)
	if code != 0 {
		return fmt.Errorf("comand exit with code %d and output: %s", code, output)
	}
	return err
}

func (tctx *testContext) stepIRunAsyncCommandOnHost(host string, body *godog.DocString) error {
	cmd := strings.TrimSpace(body.Content)
	return tctx.composer.RunAsyncCommand(host, cmd)
}

func (tctx *testContext) stepIRunCommandOnHostWithTimeout(host string, timeout int, body *godog.DocString) error {
	cmd := strings.TrimSpace(body.Content)
	var err error
	tctx.commandRetcode, tctx.commandOutput, err = tctx.composer.RunCommand(host, cmd, time.Duration(timeout)*time.Second)
	return err
}

func (tctx *testContext) stepIRunCommandOnHostUntilResultMatchWithTimeout(host string, pattern string, timeout int, body *godog.DocString) error {
	matcher, err := matchers.GetMatcher("regexp")
	if err != nil {
		return err
	}

	var lastError error
	testutil.Retry(func() bool {
		cmd := strings.TrimSpace(body.Content)
		tctx.commandRetcode, tctx.commandOutput, lastError = tctx.composer.RunCommand(host, cmd, time.Duration(timeout)*time.Second)
		if lastError != nil {
			return false
		}
		lastError = matcher(tctx.commandOutput, strings.TrimSpace(pattern))
		return lastError == nil
	}, time.Duration(timeout)*time.Second, time.Second)

	return lastError
}

func (tctx *testContext) stepIRunCommandOnHostUntilReturnCodeWithTimeout(host string, code int, timeout int, body *godog.DocString) error {
	var lastError error
	testutil.Retry(func() bool {
		cmd := strings.TrimSpace(body.Content)
		tctx.commandRetcode, tctx.commandOutput, lastError = tctx.composer.RunCommand(host, cmd, time.Duration(timeout)*time.Second)
		if lastError != nil {
			return false
		}
		return tctx.commandRetcode == code
	}, time.Duration(timeout)*time.Second, time.Second)

	return lastError
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
	_, err := tctx.queryPostgresql(host, query, struct{}{})
	return err
}

func (tctx *testContext) stepIRunSQLOnHostExpectingErrorOfNumber(host string, errorNumber int, body *godog.DocString) error {
	query := strings.TrimSpace(body.Content)
	_, err := tctx.queryPostgresql(host, query, struct{}{})
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return err
	}
	num := uint16(errorNumber)
	if mysqlErr.Number == num {
		return nil
	}
	return err
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

func (tctx *testContext) stepBreakReplicationOnHost(host string) error {
	query := fmt.Sprintf("STOP SLAVE IO_THREAD FOR CHANNEL '%s'", replicationChannel)
	if _, err := tctx.queryMysql(host, query, nil); err != nil {
		return err
	}
	query = fmt.Sprintf("STOP SLAVE FOR CHANNEL '%s'", replicationChannel)
	if _, err := tctx.queryMysql(host, query, nil); err != nil {
		return err
	}
	query = fmt.Sprintf("CHANGE MASTER TO MASTER_PASSWORD = 'incorrect' FOR CHANNEL '%s'", replicationChannel)
	if _, err := tctx.queryMysql(host, query, nil); err != nil {
		return err
	}
	query = fmt.Sprintf("START SLAVE FOR CHANNEL '%s'", replicationChannel)
	if _, err := tctx.queryMysql(host, query, nil); err != nil {
		return err
	}
	return nil
}

func (tctx *testContext) stepBreakReplicationOnHostInARepairableWay(host string) error {
	// Kill Replication IO thread:
	query := "SELECT id FROM information_schema.processlist WHERE state = 'Waiting for master to send event' OR state = 'Waiting for source to send event'"
	queryReqult, err := tctx.queryMysql(host, query, struct{}{})
	if err != nil {
		return err
	}
	if _, err := tctx.queryMysql(host, fmt.Sprintf("KILL %s", queryReqult[0]["id"]), struct{}{}); err != nil {
		return err
	}
	return nil
}

func (tctx *testContext) stepISaveSQLResultAs(varname string) error {
	tctx.variables[varname] = tctx.sqlQueryResult
	return nil
}

func (tctx *testContext) stepISaveCommandOutputAs(varname string) error {
	tctx.variables[varname] = strings.TrimSpace(tctx.commandOutput)
	return nil
}

func (tctx *testContext) stepISaveValAs(val, varname string) error {
	tctx.variables[varname] = val
	return nil
}

func (tctx *testContext) stepIWaitFor(timeout int) error {
	time.Sleep(time.Duration(timeout) * time.Second)
	return nil
}

func (tctx *testContext) stepInfoFileOnHostMatch(filepath, host, matcher string, body *godog.DocString) error {
	m, err := matchers.GetMatcher(matcher)
	if err != nil {
		return err
	}

	testutil.Retry(func() bool {
		remoteFile, err := tctx.composer.GetFile(host, filepath)
		if err != nil {
			return true
		}
		content, err := io.ReadAll(remoteFile)
		if err != nil {
			return true
		}
		err = remoteFile.Close()
		if err != nil {
			return true
		}

		if err = m(string(content), strings.TrimSpace(body.Content)); err != nil {
			return true
		}
		return false
	}, time.Second*10, time.Second)

	return err
}

// nolint: unused
func InitializeScenario(s *godog.ScenarioContext) {
	tctx, err := newTestContext()
	if err != nil {
		// TODO: how to report errors in godog
		panic(err)
	}

	s.Before(func(ctx context.Context, scenario *godog.Scenario) (context.Context, error) {
		tctx.cleanup()
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
	s.Step(`^cluster environment is$`, tctx.stepClusterEnvironmentIs)
	s.Step(`^cluster is up and running$`, func() error { return tctx.stepClusterIsUpAndRunning(true) })
	s.Step(`^cluster is up and running with clean zk$`, func() error { return tctx.stepClusterIsUpAndRunning(false) })
	s.Step(`^host "([^"]*)" is stopped$`, tctx.stepHostIsStopped)
	s.Step(`^host "([^"]*)" is detached from the network$`, tctx.stepHostIsDetachedFromTheNetwork)
	s.Step(`^host "([^"]*)" is started$`, tctx.stepHostIsStarted)
	s.Step(`^host "([^"]*)" is attached to the network$`, tctx.stepHostIsAttachedToTheNetwork)

	// host checking
	s.Step(`^host "([^"]*)" should have file "([^"]*)"$`, tctx.stepHostShouldHaveFile)
	s.Step(`^host "([^"]*)" should have file "([^"]*)" within "(\d+)" seconds$`, tctx.stepHostShouldHaveFileWithin)

	// command and SQL execution
	s.Step(`^I run command on host "([^"]*)"$`, tctx.stepIRunCommandOnHost)
	s.Step(`^I run command on host "([^"]*)" with timeout "(\d+)" seconds$`, tctx.stepIRunCommandOnHostWithTimeout)
	s.Step(`^I run async command on host "([^"]*)"$`, tctx.stepIRunAsyncCommandOnHost)
	s.Step(`^I run command on host "([^"]*)" until result match regexp "([^"]*)" with timeout "(\d+)" seconds$`, tctx.stepIRunCommandOnHostUntilResultMatchWithTimeout)
	s.Step(`^I run command on host "([^"]*)" until return code is "([^"]*)" with timeout "(\d+)" seconds$`, tctx.stepIRunCommandOnHostUntilReturnCodeWithTimeout)
	s.Step(`^command return code should be "(\d+)"$`, tctx.stepCommandReturnCodeShouldBe)
	s.Step(`^command output should match (\w+)$`, tctx.stepCommandOutputShouldMatch)
	s.Step(`^I run SQL on host "([^"]*)"$`, tctx.stepIRunSQLOnHost)
	s.Step(`^I run SQL on host "([^"]*)" expecting error on number "(\d+)"$`, tctx.stepIRunSQLOnHostExpectingErrorOfNumber)
	s.Step(`^SQL result should match (\w+)$`, tctx.stepSQLResultShouldMatch)
	s.Step(`^I break replication on host "([^"]*)"$`, tctx.stepBreakReplicationOnHost)
	s.Step(`^I break replication on host "([^"]*)" in repairable way$`, tctx.stepBreakReplicationOnHostInARepairableWay)
	s.Step(`^I set used space on host "([^"]*)" to (\d+)%$`, tctx.stepSetUsedSpace)
	s.Step(`^I set readonly file system on host "([^"]*)" to "([^"]*)"$`, tctx.stepSetReadonlyStatus)

	// variables
	s.Step(`^I save command output as "([^"]*)"$`, tctx.stepISaveCommandOutputAs)
	s.Step(`^I save SQL result as "([^"]*)"$`, tctx.stepISaveSQLResultAs)
	s.Step(`^I save "([^"]*)" as "([^"]*)"$`, tctx.stepISaveValAs)

	// misc
	s.Step(`^I wait for "(\d+)" seconds$`, tctx.stepIWaitFor)
	s.Step(`^info file "([^"]*)" on "([^"]*)" match (\w+)$`, tctx.stepInfoFileOnHostMatch)
	s.Step(`^host "([^"]*)" should have no file "([^"]*)"$`, tctx.stepHostShouldHaveNoFile)
	s.Step(`^file "([^"]*)" on host "([^"]*)" should have content$`, tctx.stepFileOnHostHaveContentOf)
}

func TestMysync(t *testing.T) {
	features := "features"
	if feauterEnv, ok := os.LookupEnv("GODOG_FEATURE"); ok {
		if !strings.HasSuffix(feauterEnv, ".feature") {
			feauterEnv += ".feature"
		}
		features = fmt.Sprintf("features/%s", feauterEnv)
	}
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
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
