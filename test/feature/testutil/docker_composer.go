package testutil

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const defaultDockerTimeout = 30 * time.Second
const defaultDockerComposeTimeout = 90 * time.Second
const defaultContainerStopTimeout = 30 * time.Second
const shell = "/bin/bash"

// Composer manipulate images/vm's during integration tests
type Composer interface {
	// Brings all containers/VMs up according to config
	Up(env []string) error
	// Trears all containers/VMs dowwn
	Down() error
	// Returns names/ids of running containers
	Services() []string
	// Returns real exposed addr (ip:port) for given service/port
	GetAddr(service string, port int) (string, error)
	// Returns internal ip address of given service
	GetIP(service string) (string, error)
	// Stops container/VM
	Stop(service string) error
	// Starts container/VM
	Start(service string) error
	// Detachs container/VM from network
	DetachFromNet(service string) error
	// Attachs container/VM to network
	AttachToNet(service string) error
	// Executes command inside container/VM with given timeout.
	// Returns command retcode and output (stdoud and stderr are mixed)
	RunCommand(service, cmd string, timeout time.Duration) (retcode int, output string, err error)
	RunCommandAtHosts(cmd, hostsSubstring string, timeout time.Duration) error
	// Executes command inside container/VM with given timeout.
	// Returns command retcode and output (stdoud and stderr are mixed)
	RunAsyncCommand(service, cmd string) error
	// Returns content of the file from container by path
	GetFile(service, path string) (io.ReadCloser, error)
	// CheckIfFileExist Checks if file exists
	CheckIfFileExist(service, path string) (bool, error)
}

// DockerComposer is a Composer implementation based on docker and docker-compose
type DockerComposer struct {
	projectName string
	config      string
	api         *client.Client
	containers  map[string]types.Container
	stopped     map[string]bool
}

// NewDockerComposer returns DockerComposer instance for specified compose file
// Parameter project specify prefix to distguish docker container and networks from different runs
func NewDockerComposer(project, config string) (*DockerComposer, error) {
	if config == "" {
		config = "docker-compose.yaml"
	}
	config, err := filepath.Abs(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build abs path to compose file: %s", err)
	}
	if project == "" {
		project = filepath.Base(filepath.Dir(config))
	}
	dc := new(DockerComposer)
	api, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to docker: %s", err)
	}
	dc.api = api
	dc.config = config
	dc.projectName = fmt.Sprintf("%s-%d", project, os.Getpid())
	dc.containers = make(map[string]types.Container)
	dc.stopped = make(map[string]bool)
	return dc, nil
}

func (dc *DockerComposer) runCompose(args []string, env []string) error {
	args2 := []string{}
	args2 = append(args2, "compose", "-f", dc.config, "-p", dc.projectName)
	args2 = append(args2, args...)
	cmd := exec.Command("docker", args2...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run 'docker %s': %s\n%s", strings.Join(args2, " "), err, out)
	}
	return nil
}

func (dc *DockerComposer) fillContainers() error {
	containers, err := dc.api.ContainerList(context.Background(), container.ListOptions{All: true})
	if err != nil {
		return err
	}
	errorFlag := false
	var name, state string
	for _, c := range containers { // nolint: gocritic
		prj := c.Labels["com.docker.compose.project"]
		srv := c.Labels["com.docker.compose.service"]
		if prj != dc.projectName || srv == "" {
			continue
		}
		dc.containers[srv] = c

		if c.State != "running" && !dc.stopped[srv] {
			errorFlag = true
			name = srv
			state = c.State
		}
	}
	if errorFlag {
		return fmt.Errorf("container %s is %s, not running", name, state)
	}
	return nil
}

// Up brings all containers up according to config
func (dc *DockerComposer) Up(env []string) error {
	err := dc.runCompose([]string{"up", "-d", "--force-recreate", "-t", strconv.Itoa(int(defaultDockerComposeTimeout / time.Second))}, env)
	if err != nil {
		// to save container logs
		_ = dc.fillContainers()
		return err
	}
	err = dc.fillContainers()
	return err
}

// Down trears all containers/VMs dowwn
func (dc *DockerComposer) Down() error {
	return dc.runCompose([]string{"down", "-v" /* "-t", strconv.Itoa(int(defaultDockerComposeTimeout / time.Second))*/}, nil)
}

// Services returns names/ids of running containers
func (dc *DockerComposer) Services() []string {
	services := make([]string, 0, len(dc.containers))
	for s := range dc.containers {
		services = append(services, s)
	}
	sort.Strings(services)
	return services
}

// GetAddr returns real exposed addr (ip:port) for given service/port
func (dc *DockerComposer) GetAddr(service string, port int) (string, error) {
	cont, ok := dc.containers[service]
	if !ok {
		return "", fmt.Errorf("no such service: %s", service)
	}
	for _, p := range cont.Ports {
		if int(p.PrivatePort) == port {
			return net.JoinHostPort(p.IP, strconv.Itoa(int(p.PublicPort))), nil
		}
	}
	return "", fmt.Errorf("service %s does not expose port %d", service, port)
}

// GetIp returns internal ip address of given service
func (dc *DockerComposer) GetIP(service string) (string, error) {
	cont, ok := dc.containers[service]
	if !ok {
		return "", fmt.Errorf("no such service: %s", service)
	}
	for _, network := range cont.NetworkSettings.Networks {
		return network.IPAddress, nil
	}
	return "", fmt.Errorf("no network for service: %s", service)
}

func (dc *DockerComposer) RunCommandAtHosts(cmd, hostSubstring string, timeout time.Duration) error {
	for name := range dc.containers {
		if !strings.Contains(name, hostSubstring) {
			continue
		}
		_, _, err := dc.RunCommand(name, cmd, timeout)
		if err != nil {
			return err
		}
	}
	return nil
}

// RunCommand executes command inside container/VM with given timeout.
func (dc *DockerComposer) RunCommand(service string, cmd string, timeout time.Duration) (retcode int, out string, err error) {
	cont, ok := dc.containers[service]
	if !ok {
		return 0, "", fmt.Errorf("no such service: %s", service)
	}
	if timeout == 0 {
		timeout = defaultDockerTimeout + 3*time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	execCfg := types.ExecConfig{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          []string{shell, "-c", cmd},
	}
	execResp, err := dc.api.ContainerExecCreate(ctx, cont.ID, execCfg)
	if err != nil {
		return 0, "", err
	}
	attachResp, err := dc.api.ContainerExecAttach(ctx, execResp.ID, types.ExecStartCheck{})
	if err != nil {
		return 0, "", err
	}
	var outBuf, errBuf bytes.Buffer
	_, err = stdcopy.StdCopy(&outBuf, &errBuf, attachResp.Reader)
	if err != nil {
		return 0, "", fmt.Errorf("failed demultiplexing exec output")
	}
	output, err := io.ReadAll(&outBuf)
	attachResp.Close()
	if err != nil {
		return 0, "", err
	}
	var insp types.ContainerExecInspect
	Retry(func() bool {
		insp, err = dc.api.ContainerExecInspect(ctx, execResp.ID)
		return err != nil || !insp.Running
	}, timeout, time.Second)
	if err != nil {
		return 0, "", err
	}
	if insp.Running {
		return 0, "", fmt.Errorf("command %s didn't returned within %s", cmd, timeout)
	}
	return insp.ExitCode, string(output), nil
}

// RunAsyncCommand executes command inside container/VM without waiting for termination.
func (dc *DockerComposer) RunAsyncCommand(service string, cmd string) error {
	cont, ok := dc.containers[service]
	if !ok {
		return fmt.Errorf("no such service: %s", service)
	}
	execCfg := types.ExecConfig{
		Detach: true,
		Cmd:    []string{shell, "-c", cmd},
	}
	execResp, err := dc.api.ContainerExecCreate(context.Background(), cont.ID, execCfg)
	if err != nil {
		return err
	}
	return dc.api.ContainerExecStart(context.Background(), execResp.ID, types.ExecStartCheck{})
}

// GetFile returns content of the fail from continer by path
func (dc *DockerComposer) GetFile(service, path string) (io.ReadCloser, error) {
	cont, ok := dc.containers[service]
	if !ok {
		return nil, fmt.Errorf("no such service: %s", service)
	}
	reader, _, err := dc.api.CopyFromContainer(context.Background(), cont.ID, path)
	if err != nil {
		return nil, err
	}
	return newUntarReaderCloser(reader)
}

// CheckIfFileExist Checks if file exists
func (dc *DockerComposer) CheckIfFileExist(service, path string) (bool, error) {
	cont, ok := dc.containers[service]
	if !ok {
		return false, fmt.Errorf("no such service: %s", service)
	}
	_, err := dc.api.ContainerStatPath(context.Background(), cont.ID, path)
	if err != nil {
		if client.IsErrNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Start starts container by service name
func (dc *DockerComposer) Start(service string) error {
	cont, ok := dc.containers[service]
	if !ok {
		return fmt.Errorf("no such service: %s", service)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultDockerTimeout)
	defer cancel()
	stopTimeout := int(defaultContainerStopTimeout)
	err := dc.api.ContainerRestart(ctx, cont.ID,
		container.StopOptions{
			Signal:  "",
			Timeout: &stopTimeout,
		})
	if err != nil {
		return err
	}
	delete(dc.stopped, service)
	// to update exposed ports
	return dc.fillContainers()
}

// Stop stops container by service name
func (dc *DockerComposer) Stop(service string) error {
	cont, ok := dc.containers[service]
	if !ok {
		return fmt.Errorf("no such service: %s", service)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultDockerTimeout)
	defer cancel()
	stopTimeout := int(defaultContainerStopTimeout)
	err := dc.api.ContainerStop(ctx, cont.ID, container.StopOptions{
		Signal:  "",
		Timeout: &stopTimeout,
	})
	dc.stopped[service] = true
	return err
}

// AttachToNet attachs container to network
func (dc *DockerComposer) AttachToNet(service string) error {
	_, ok := dc.containers[service]
	if !ok {
		return fmt.Errorf("no such service: %s", service)
	}
	cmds := []string{
		"iptables -D INPUT -i eth0 -j DROP",
		"iptables -D OUTPUT -o eth0 -j DROP",
		"ip6tables -D INPUT -i eth0 -j DROP",
		"ip6tables -D OUTPUT -o eth0 -j DROP",
	}
	for _, cmd := range cmds {
		_, _, err := dc.RunCommand(service, cmd, defaultDockerTimeout)
		if err != nil {
			return err
		}
	}
	return nil
}

// DetachFromNet detaches container from network
func (dc *DockerComposer) DetachFromNet(service string) error {
	_, ok := dc.containers[service]
	if !ok {
		return fmt.Errorf("no such service: %s", service)
	}
	cmds := []string{
		"iptables -A INPUT -i eth0 -j DROP",
		"iptables -A OUTPUT -o eth0 -j DROP",
		"ip6tables -A INPUT -i eth0 -j DROP",
		"ip6tables -A OUTPUT -o eth0 -j DROP",
	}
	for _, cmd := range cmds {
		_, _, err := dc.RunCommand(service, cmd, defaultDockerTimeout)
		if err != nil {
			return err
		}
	}
	return nil
}

func newUntarReaderCloser(reader io.ReadCloser) (io.ReadCloser, error) {
	tarReader := tar.NewReader(reader)
	_, err := tarReader.Next()
	if err != nil {
		return nil, err
	}
	return &untarReaderCloser{tarReader, reader}, nil
}

type untarReaderCloser struct {
	tarReader  *tar.Reader
	baseReader io.ReadCloser
}

func (usf untarReaderCloser) Read(b []byte) (int, error) {
	return usf.tarReader.Read(b)
}

func (usf untarReaderCloser) Close() error {
	return usf.baseReader.Close()
}
