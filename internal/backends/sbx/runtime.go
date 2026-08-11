package sbxbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dockerbackend "e2b-local/internal/backends/docker"
	"e2b-local/internal/e2bapi"
	gateway "e2b-local/internal/gateway"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
)

const (
	sbxManagedEnv          = "E2B_SBX_MANAGED"
	sbxModeEnv             = "E2B_SBX_MODE"
	sbxSandboxIDEnv        = "E2B_SBX_SANDBOX_ID"
	sbxTemplateIDEnv       = "E2B_SBX_TEMPLATE_ID"
	sbxMetadataEnv         = "E2B_SBX_METADATA"
	sbxCreatedAtEnv        = "E2B_SBX_CREATED_AT"
	sbxEndAtEnv            = "E2B_SBX_END_AT"
	sbxAllowInternetEnv    = "E2B_SBX_ALLOW_INTERNET"
	sbxVolumeMountsEnv     = "E2B_SBX_VOLUME_MOUNTS"
	sbxBootstrapVolumesEnv = "E2B_SBX_BOOTSTRAP_VOLUMES"
	sbxTunnelsEnv          = "E2B_SBX_TUNNELS"
	sbxStartCommandEnv     = "E2B_SBX_ENVD_START_CMD"
	sbxEnvdPortEnv         = "E2B_SBX_ENVD_PORT"
	sbxManagedLabel        = "e2b.local.sbx.managed"
	defaultSandboxTimeout  = gateway.DefaultSandboxTimeoutSeconds
)

type sbxMode string

const (
	sbxModeSandboxd sbxMode = "sandboxd"
	sbxModeDegraded sbxMode = "degraded"
)

// SbxRuntime keeps the same constructor layout as the existing VM runtimes:
// configuration, logger, HTTP health check, and injectable identifiers are
// grouped first, while protocol clients remain private implementation details.
type SbxRuntime struct {
	cfg          SbxRuntimeConfig
	logger       *log.Logger
	httpClient   *http.Client
	checkHealthy func(ctx context.Context, envdURL string) error
	now          func() time.Time
	newID        func() string

	dockerClient   *client.Client
	sandboxdClient sandboxdAPI
	metricsClient  metricsAPI
	volumeRuntime  *dockerbackend.DockerRuntime
	tunnelHost     func() (string, error)

	loginMu        sync.Mutex
	loginKnown     bool
	loginAvailable bool
	loginCheckedAt time.Time

	relayMu sync.Mutex
	relays  map[string][]*TunnelRelay

	metricMu   sync.Mutex
	lastMetric map[string]sbxMetricPoint
	stateMu    sync.Mutex
}

type sbxMetricPoint struct {
	timestamp time.Time
	busy      float64
	total     float64
}

type sbxVolumeSpec struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

func init() {
	gateway.RegisterSandboxRuntimeFactory("sbx", func(cfg gateway.Config, logger *log.Logger) (gateway.SandboxRuntime, error) {
		return NewSbxRuntime(cfg.Sbx, logger)
	})
}

func NewSbxRuntime(cfg SbxRuntimeConfig, logger *log.Logger) (*SbxRuntime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	dockerClient, err := client.NewClientWithOpts(
		client.WithHost(sbxDockerHost(cfg.DockerSocket)),
		client.WithVersion(cfg.DockerAPIVersion),
	)
	if err != nil {
		return nil, fmt.Errorf("create sbx docker client: %w", err)
	}
	volumeCfg := gateway.DefaultConfig().Docker
	volumeCfg.Host = sbxDockerHost(cfg.DockerSocket)
	volumeCfg.APIVersion = cfg.DockerAPIVersion
	volumeCfg.ContainerNamePrefix = cfg.ContainerNamePrefix
	volumeCfg.VolumeHostPath = cfg.VolumeHostPath
	volumeRuntime, err := dockerbackend.NewDockerRuntime(volumeCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create sbx volume runtime: %w", err)
	}

	runtime := &SbxRuntime{
		cfg:            cfg,
		logger:         logger,
		httpClient:     &http.Client{Timeout: 3 * time.Second},
		now:            time.Now,
		newID:          uuid.NewString,
		dockerClient:   dockerClient,
		sandboxdClient: newSandboxdClient(cfg.SandboxdSocket),
		metricsClient:  newMetricsClient(cfg.MetricsRoot),
		volumeRuntime:  volumeRuntime,
		relays:         map[string][]*TunnelRelay{},
		lastMetric:     map[string]sbxMetricPoint{},
	}
	runtime.tunnelHost = func() (string, error) {
		if host := strings.TrimSpace(runtime.cfg.TunnelHost); host != "" {
			return host, nil
		}
		return gateway.DetectOutboundHost("8.8.8.8:80")
	}
	runtime.checkHealthy = runtime.defaultHealthCheck
	return runtime, nil
}

func sbxDockerHost(socket string) string {
	socket = strings.TrimSpace(socket)
	if strings.Contains(socket, "://") {
		return socket
	}
	return "unix://" + socket
}

func (r *SbxRuntime) CreateSandbox(ctx context.Context, req SandboxRuntimeCreateRequest) (SandboxRuntimeInfo, error) {
	mode, err := r.lifecycleMode(ctx)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	template, err := r.resolveTemplate(ctx, req.TemplateID)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	volumeMounts, bootstrapVolumes, workspaces, directMounts, err := r.resolveVolumeMounts(req.VolumeMounts)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	relays, tunnelSpecs, err := r.startRelays()
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	cleanupRelays := true
	defer func() {
		if cleanupRelays {
			closeTunnelRelays(relays)
		}
	}()

	name := strings.TrimSpace(r.cfg.ContainerNamePrefix) + strings.TrimSpace(req.SandboxID)
	if strings.TrimSpace(req.SandboxID) == "" {
		return SandboxRuntimeInfo{}, fmt.Errorf("sandbox id is required")
	}
	if mode == sbxModeDegraded {
		// The degraded path uses Docker bind mounts directly. Host paths must not
		// be passed to the guest bootstrap, where they would be invalid.
		bootstrapVolumes = nil
	}
	env, err := r.runtimeEnvironment(req, template, mode, volumeMounts, bootstrapVolumes, tunnelSpecs)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}

	info := SandboxRuntimeInfo{
		SandboxID:     req.SandboxID,
		ContainerName: name,
		VolumeMounts:  volumeMounts,
	}
	info = r.runtimeInfoWithTunnels(info, tunnelSpecs)
	switch mode {
	case sbxModeSandboxd:
		workspace := r.cfg.Workspace
		additionalWorkspaces := []sandboxdAdditionalWorkspace(nil)
		if len(workspaces) > 0 {
			workspace = workspaces[0]
			additionalWorkspaces = make([]sandboxdAdditionalWorkspace, 0, len(workspaces)-1)
			for _, source := range workspaces[1:] {
				additionalWorkspaces = append(additionalWorkspaces, sandboxdAdditionalWorkspace{Dir: source})
			}
		}
		created, err := r.sandboxdClient.CreateSandbox(ctx, sandboxdCreateRequest{
			Agent:                r.cfg.Agent,
			Workspace:            workspace,
			Template:             template.Image,
			Name:                 name,
			Memory:               template.Memory,
			CPUs:                 template.CPUs,
			AdditionalWorkspaces: additionalWorkspaces,
			Env:                  env,
		})
		if err != nil {
			return SandboxRuntimeInfo{}, fmt.Errorf("create sandboxd sandbox: %w", err)
		}
		info.MachineID = created.ID
		if created.Name != "" {
			info.ContainerName = created.Name
		}
		if info.MachineID == "" {
			// sandboxd's create response may omit its internal UUID. The sandbox
			// name is the stable lifecycle handle for every authenticated call.
			info.MachineID = info.ContainerName
		}
		containerInfo, err := r.waitForSandboxContainer(ctx, info.ContainerName)
		if err != nil {
			_ = r.sandboxdClient.DeleteSandbox(context.Background(), info.ContainerName)
			return SandboxRuntimeInfo{}, err
		}
		info.ContainerID = containerInfo.ID
		if err := r.startGuestServices(ctx, info.ContainerID, env); err != nil {
			_ = r.sandboxdClient.DeleteSandbox(context.Background(), info.ContainerName)
			return SandboxRuntimeInfo{}, err
		}
	case sbxModeDegraded:
		created, err := r.dockerClient.ContainerCreate(ctx,
			&container.Config{
				Image:    template.Image,
				Env:      environmentList(env),
				Labels:   map[string]string{sbxManagedLabel: "true"},
				Hostname: name,
			},
			&container.HostConfig{Mounts: directMounts},
			nil,
			nil,
			name,
		)
		if err != nil {
			return SandboxRuntimeInfo{}, fmt.Errorf("create degraded sbx container: %w", err)
		}
		info.ContainerID = created.ID
		if err := r.dockerClient.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			_ = r.removeContainer(context.Background(), created.ID)
			return SandboxRuntimeInfo{}, fmt.Errorf("start degraded sbx container: %w", err)
		}
	default:
		return SandboxRuntimeInfo{}, fmt.Errorf("unsupported sbx lifecycle mode %q", mode)
	}

	r.registerRelays(info.ContainerID, relays)
	cleanupRelays = false
	info, err = r.inspectRuntimeInfo(ctx, info)
	if err != nil {
		_ = r.DeleteSandbox(context.Background(), info)
		return SandboxRuntimeInfo{}, err
	}
	if mode == sbxModeSandboxd {
		if err := r.saveRuntimeState(newSbxRuntimeState(req, info, mode, env, tunnelSpecs)); err != nil {
			_ = r.DeleteSandbox(context.Background(), info)
			return SandboxRuntimeInfo{}, err
		}
	}
	if err := r.checkHealthy(ctx, info.EnvdURL); err != nil {
		_ = r.DeleteSandbox(context.Background(), info)
		return SandboxRuntimeInfo{}, err
	}

	r.logger.Printf("sbx sandbox started sandbox_id=%s mode=%s container_id=%s container_name=%s envd_url=%s",
		req.SandboxID, mode, info.ContainerID, info.ContainerName, info.EnvdURL)
	return info, nil
}

func (r *SbxRuntime) ListTemplates(ctx context.Context) ([]SandboxRuntimeTemplate, error) {
	if _, err := r.lifecycleMode(ctx); err != nil {
		return nil, err
	}
	templates := make([]SandboxRuntimeTemplate, 0, len(r.cfg.Templates))
	for templateID, template := range r.cfg.Templates {
		inspect, _, err := r.dockerClient.ImageInspectWithRaw(ctx, template.Image)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("inspect sbx template image %q: %w", template.Image, err)
		}
		createdAt := r.now().UTC()
		if value := strings.TrimSpace(inspect.Created); value != "" {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, value); parseErr == nil {
				createdAt = parsed.UTC()
			}
		}
		templates = append(templates, SandboxRuntimeTemplate{
			TemplateID:  templateID,
			Names:       []string{templateID},
			ImageRef:    template.Image,
			BuildCount:  1,
			BuildID:     shortSbxImageID(inspect.ID),
			BuildStatus: "ready",
			CPUCount:    template.CPUs,
			DiskSizeMB:  bytesToMB(inspect.Size),
			MemoryMB:    memoryMB(template.Memory),
			CreatedAt:   createdAt,
			UpdatedAt:   createdAt,
		})
	}
	sort.Slice(templates, func(i, j int) bool { return templates[i].TemplateID < templates[j].TemplateID })
	return templates, nil
}

func (r *SbxRuntime) DeleteSandbox(ctx context.Context, info SandboxRuntimeInfo) error {
	if r.isSandboxdManaged(info) {
		if err := r.sandboxdClient.DeleteSandbox(ctx, info.ContainerName); err != nil && !sandboxdHasStatus(err, http.StatusNotFound) {
			return fmt.Errorf("delete sandboxd sandbox: %w", err)
		}
	} else if err := r.removeContainer(ctx, info.ContainerID); err != nil {
		return err
	}
	r.closeRelays(info.ContainerID)
	if err := r.removeRuntimeState(info.SandboxID); err != nil {
		return err
	}
	r.logger.Printf("sbx sandbox removed container_id=%s container_name=%s", info.ContainerID, info.ContainerName)
	return nil
}

func (r *SbxRuntime) PauseSandbox(ctx context.Context, info SandboxRuntimeInfo) error {
	if r.isSandboxdManaged(info) {
		if err := r.sandboxdClient.StopSandbox(ctx, info.ContainerName); err != nil && !sandboxdHasStatus(err, http.StatusConflict) {
			return fmt.Errorf("stop sandboxd sandbox: %w", err)
		}
		return nil
	}
	if info.ContainerID == "" {
		return nil
	}
	if err := r.dockerClient.ContainerStop(ctx, info.ContainerID, container.StopOptions{}); err != nil && !errdefs.IsNotModified(err) {
		return fmt.Errorf("stop degraded sbx container: %w", err)
	}
	return nil
}

func (r *SbxRuntime) ResumeSandbox(ctx context.Context, info SandboxRuntimeInfo) (SandboxRuntimeInfo, error) {
	state, hasState, err := r.loadRuntimeState(info.SandboxID)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	if hasState {
		info = mergeSbxRuntimeInfo(info, state.RuntimeInfo)
		if state.Mode == sbxModeSandboxd && info.MachineID == "" {
			info.MachineID = info.ContainerName
		}
	}
	if r.isSandboxdManaged(info) {
		if !hasState {
			return SandboxRuntimeInfo{}, fmt.Errorf("missing sbx runtime state for sandbox %s; recreate the sandbox", info.SandboxID)
		}
		started := true
		if err := r.sandboxdClient.StartSandbox(ctx, info.ContainerName); err != nil {
			if !sandboxdHasStatus(err, http.StatusConflict) {
				return SandboxRuntimeInfo{}, fmt.Errorf("start sandboxd sandbox: %w", err)
			}
			started = false
		}
		containerInfo, err := r.waitForSandboxContainer(ctx, info.ContainerName)
		if err != nil {
			return SandboxRuntimeInfo{}, err
		}
		previousContainerID := info.ContainerID
		info.ContainerID = containerInfo.ID
		r.moveRelays(previousContainerID, info.ContainerID)
		if err := r.waitForContainerRunning(ctx, info.ContainerID); err != nil {
			return SandboxRuntimeInfo{}, err
		}
		info = r.runtimeInfoWithTunnels(info, state.Tunnels)
		if err := r.restoreRelaysForTunnels(info.ContainerID, state.Tunnels); err != nil {
			return SandboxRuntimeInfo{}, err
		}
		if started {
			if err := r.startGuestServices(ctx, info.ContainerID, state.Environment); err != nil {
				return SandboxRuntimeInfo{}, err
			}
		}
	} else if info.ContainerID != "" {
		if err := r.dockerClient.ContainerStart(ctx, info.ContainerID, container.StartOptions{}); err != nil && !errdefs.IsConflict(err) {
			return SandboxRuntimeInfo{}, fmt.Errorf("start degraded sbx container: %w", err)
		}
	}

	resumed, err := r.inspectRuntimeInfo(ctx, info)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	if err := r.checkHealthy(ctx, resumed.EnvdURL); err != nil {
		return SandboxRuntimeInfo{}, err
	}
	if hasState {
		state.RuntimeInfo = resumed
		if err := r.saveRuntimeState(state); err != nil {
			return SandboxRuntimeInfo{}, err
		}
	}
	return resumed, nil
}

func (r *SbxRuntime) InspectSandbox(ctx context.Context, info SandboxRuntimeInfo) (SandboxRuntimeInspection, error) {
	containerID := strings.TrimSpace(info.ContainerID)
	if containerID == "" {
		containerID = strings.TrimSpace(info.ContainerName)
	}
	if containerID == "" {
		return SandboxRuntimeInspection{Info: info, Exists: false}, nil
	}
	inspect, err := r.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return SandboxRuntimeInspection{Info: info, Exists: false}, nil
		}
		return SandboxRuntimeInspection{}, fmt.Errorf("inspect sbx container: %w", err)
	}
	if inspect.ID != "" {
		info.ContainerID = inspect.ID
	}
	if info.ContainerName == "" {
		info.ContainerName = strings.TrimPrefix(inspect.Name, "/")
	}
	updated, err := r.runtimeInfoFromContainer(info, inspect)
	if err != nil {
		return SandboxRuntimeInspection{}, err
	}
	state := sbxContainerState(inspect.State)
	if state == "" {
		return SandboxRuntimeInspection{Info: updated, Exists: false}, nil
	}
	return SandboxRuntimeInspection{Info: updated, State: state, Exists: true}, nil
}

func (r *SbxRuntime) RestoreSandboxes(ctx context.Context) ([]SandboxRecord, error) {
	stateRecords, err := r.restoreSandboxdStates(ctx)
	if err != nil {
		return nil, err
	}
	containers, err := r.dockerClient.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("list sbx containers: %w", err)
	}
	records := make([]SandboxRecord, 0, len(stateRecords)+len(containers))
	records = append(records, stateRecords...)
	restoredIDs := make(map[string]struct{}, len(stateRecords))
	for _, record := range stateRecords {
		restoredIDs[record.ID] = struct{}{}
	}
	for _, summary := range containers {
		inspect, err := r.dockerClient.ContainerInspect(ctx, summary.ID)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("inspect sbx container %s: %w", summary.ID, err)
		}
		var environment []string
		if inspect.Config != nil {
			environment = inspect.Config.Env
		}
		env := environmentMap(environment)
		if env[sbxManagedEnv] != "true" {
			continue
		}
		record, ok, err := r.restoreSandboxRecord(inspect, env)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if _, restored := restoredIDs[record.ID]; restored {
			continue
		}
		if err := r.restoreRelays(record.RuntimeInfo.ContainerID, env); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt.Before(records[j].CreatedAt) })
	return records, nil
}

func (r *SbxRuntime) GetSandboxLogs(ctx context.Context, info SandboxRuntimeInfo, req SandboxLogsRequest) ([]SandboxRuntimeLogEntry, error) {
	limit := req.Limit
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	command := fmt.Sprintf("if [ -r /var/log/envd.log ]; then tail -n %d /var/log/envd.log; elif command -v journalctl >/dev/null 2>&1; then journalctl --no-pager -n %d; elif [ -r /var/log/syslog ]; then tail -n %d /var/log/syslog; fi", limit, limit, limit)
	output, err := r.runSandboxCommand(ctx, info, []string{"/bin/sh", "-lc", command})
	if err != nil {
		return nil, err
	}
	return sbxLogEntries(output, req, r.now()), nil
}

func (r *SbxRuntime) GetSandboxMetrics(ctx context.Context, record SandboxRecord, req SandboxMetricsRequest) ([]e2bapi.SandboxMetric, error) {
	metric := gateway.SandboxMetricFromRecord(record, r.now().UTC())
	values, err := r.metricsClient.Read(ctx, record.RuntimeInfo.ContainerID)
	if err != nil {
		return nil, err
	}
	const pageSize = 4096
	totalPages := metricValueBySuffix(values, "guest_mem_total_pages")
	availablePages := metricValueBySuffix(values, "guest_mem_available_pages", "guest_mem_free_pages")
	if totalPages > 0 {
		metric.MemTotal = int64(totalPages * pageSize)
		metric.MemUsed = int64((totalPages - availablePages) * pageSize)
		if metric.MemUsed < 0 {
			metric.MemUsed = 0
		}
	}
	metric.MemCache = int64(metricValueBySuffix(values, "guest_mem_file_pages") * pageSize)

	busy := metricValueBySuffix(values, "guest_cpu_user_jiffies_total") + metricValueBySuffix(values, "guest_cpu_system_jiffies_total")
	total := busy + metricValueBySuffix(values, "guest_cpu_idle_jiffies_total") + metricValueBySuffix(values, "guest_cpu_iowait_jiffies_total") + metricValueBySuffix(values, "guest_cpu_steal_jiffies_total")
	if total > 0 {
		r.metricMu.Lock()
		previous, ok := r.lastMetric[record.RuntimeInfo.ContainerID]
		current := sbxMetricPoint{timestamp: metric.Timestamp, busy: busy, total: total}
		r.lastMetric[record.RuntimeInfo.ContainerID] = current
		r.metricMu.Unlock()
		if ok && current.total > previous.total {
			metric.CpuUsedPct = float32((current.busy - previous.busy) / (current.total - previous.total) * 100)
			if metric.CpuUsedPct < 0 {
				metric.CpuUsedPct = 0
			}
		}
	}
	if !gateway.MetricMatchesRange(metric, req) {
		return []e2bapi.SandboxMetric{}, nil
	}
	return []e2bapi.SandboxMetric{metric}, nil
}

// OpenPTY opens the Docker Engine raw-stream channel used for interactive
// terminals. sandboxd's TTY endpoint is intentionally not used because it is
// known to hang on nerdbox.
func (r *SbxRuntime) OpenPTY(ctx context.Context, info SandboxRuntimeInfo, command []string) (dockertypes.HijackedResponse, string, error) {
	if strings.TrimSpace(info.ContainerID) == "" {
		return dockertypes.HijackedResponse{}, "", fmt.Errorf("container id is required")
	}
	if len(command) == 0 {
		return dockertypes.HijackedResponse{}, "", fmt.Errorf("pty command is required")
	}
	exec, err := r.dockerClient.ContainerExecCreate(ctx, info.ContainerID, container.ExecOptions{
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		Tty:          true,
		Cmd:          command,
	})
	if err != nil {
		return dockertypes.HijackedResponse{}, "", fmt.Errorf("create sbx pty exec: %w", err)
	}
	attached, err := r.dockerClient.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{Tty: true})
	if err != nil {
		return dockertypes.HijackedResponse{}, "", fmt.Errorf("attach sbx pty exec: %w", err)
	}
	return attached, exec.ID, nil
}

func (r *SbxRuntime) ResizePTY(ctx context.Context, execID string, height, width uint) error {
	if strings.TrimSpace(execID) == "" {
		return fmt.Errorf("exec id is required")
	}
	return r.dockerClient.ContainerExecResize(ctx, execID, container.ResizeOptions{Height: height, Width: width})
}

func (r *SbxRuntime) CreateVolume(ctx context.Context, name string) (RuntimeVolume, error) {
	return r.volumeRuntime.CreateVolume(ctx, name)
}

func (r *SbxRuntime) ListVolumes(ctx context.Context) ([]RuntimeVolume, error) {
	return r.volumeRuntime.ListVolumes(ctx)
}

func (r *SbxRuntime) GetVolume(ctx context.Context, volumeID string) (RuntimeVolume, error) {
	return r.volumeRuntime.GetVolume(ctx, volumeID)
}

func (r *SbxRuntime) DeleteVolume(ctx context.Context, volumeID string) (bool, error) {
	return r.volumeRuntime.DeleteVolume(ctx, volumeID)
}

func (r *SbxRuntime) GetVolumePathInfo(ctx context.Context, volumeID, volumePath string) (gateway.VolumeEntryStat, error) {
	return r.volumeRuntime.GetVolumePathInfo(ctx, volumeID, volumePath)
}

func (r *SbxRuntime) ReadVolumeFile(ctx context.Context, volumeID, volumePath string) (io.ReadCloser, error) {
	return r.volumeRuntime.ReadVolumeFile(ctx, volumeID, volumePath)
}

func (r *SbxRuntime) WriteVolumeFile(ctx context.Context, volumeID, volumePath string, body io.Reader, options gateway.VolumeWriteOptions) (gateway.VolumeEntryStat, error) {
	return r.volumeRuntime.WriteVolumeFile(ctx, volumeID, volumePath, body, options)
}

func (r *SbxRuntime) ListVolumeDir(ctx context.Context, volumeID, volumePath string, depth int) ([]gateway.VolumeEntryStat, error) {
	return r.volumeRuntime.ListVolumeDir(ctx, volumeID, volumePath, depth)
}

func (r *SbxRuntime) CreateVolumeDir(ctx context.Context, volumeID, volumePath string, options gateway.VolumeWriteOptions) (gateway.VolumeEntryStat, error) {
	return r.volumeRuntime.CreateVolumeDir(ctx, volumeID, volumePath, options)
}

func (r *SbxRuntime) lifecycleMode(ctx context.Context) (sbxMode, error) {
	loggedIn, err := r.isLoggedIn(ctx)
	if err != nil {
		return "", err
	}
	if loggedIn {
		return sbxModeSandboxd, nil
	}
	if r.cfg.RequireLogin && !r.cfg.AllowDegraded {
		message := "Please sign in to Docker to use the sbx runtime (full capabilities): " + r.cfg.LoginHint
		if hint := strings.TrimSpace(r.cfg.CertHint); hint != "" {
			message += "; certificate troubleshooting: " + hint
		}
		return "", errors.New(message)
	}
	return sbxModeDegraded, nil
}

func (r *SbxRuntime) isLoggedIn(ctx context.Context) (bool, error) {
	r.loginMu.Lock()
	if r.loginKnown && r.now().Sub(r.loginCheckedAt) < 15*time.Second {
		available := r.loginAvailable
		r.loginMu.Unlock()
		return available, nil
	}
	r.loginMu.Unlock()

	available, err := r.sandboxdClient.IsAuthenticated(ctx)
	if err != nil {
		return false, fmt.Errorf("check sbx login state: %w", err)
	}
	r.loginMu.Lock()
	r.loginKnown = true
	r.loginAvailable = available
	r.loginCheckedAt = r.now()
	r.loginMu.Unlock()
	return available, nil
}

func (r *SbxRuntime) resolveTemplate(ctx context.Context, templateID string) (SbxTemplateConfig, error) {
	templateID = strings.TrimSpace(templateID)
	if templateID == "" {
		return SbxTemplateConfig{}, fmt.Errorf("templateID is required")
	}
	template, ok := r.cfg.Templates[templateID]
	if !ok {
		template = SbxTemplateConfig{Image: templateID}
	}
	if strings.TrimSpace(template.Image) == "" {
		template.Image = r.cfg.DefaultImage
	}
	if _, _, err := r.dockerClient.ImageInspectWithRaw(ctx, template.Image); err != nil {
		if errdefs.IsNotFound(err) {
			return SbxTemplateConfig{}, fmt.Errorf("sbx image %q is not available locally; rebuild and import it with scripts/build-sbx-image.sh", template.Image)
		}
		return SbxTemplateConfig{}, fmt.Errorf("inspect sbx template image %q: %w", template.Image, err)
	}
	return template, nil
}

func (r *SbxRuntime) resolveVolumeMounts(mounts []VolumeMount) ([]VolumeMount, []sbxVolumeSpec, []string, []mount.Mount, error) {
	if len(mounts) == 0 {
		return nil, nil, nil, nil, nil
	}
	normalized := make([]VolumeMount, 0, len(mounts))
	bootstrap := make([]sbxVolumeSpec, 0, len(mounts))
	workspaces := []string{}
	directMounts := make([]mount.Mount, 0, len(mounts))
	seenWorkspaces := map[string]struct{}{}
	for _, requested := range mounts {
		volumeID := strings.TrimSpace(requested.VolumeID)
		if volumeID == "" {
			volumeID = strings.TrimSpace(requested.Name)
		}
		target := strings.TrimSpace(requested.Path)
		if target == "" {
			target = strings.TrimSpace(requested.MountPath)
		}
		if volumeID == "" || target == "" {
			return nil, nil, nil, nil, fmt.Errorf("sbx volume mount requires volume id and path")
		}
		if !path.IsAbs(target) || target == "/" {
			return nil, nil, nil, nil, fmt.Errorf("sbx volume mount path must be a non-root absolute path: %s", target)
		}
		volume, source, err := r.volumeRuntime.EnsureLocalVolumeHostDir(volumeID)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		normalized = append(normalized, VolumeMount{VolumeID: volume.VolumeID, Name: volume.Name, Path: target, MountPath: target})
		bootstrap = append(bootstrap, sbxVolumeSpec{Source: source, Target: target})
		if _, exists := seenWorkspaces[source]; !exists {
			seenWorkspaces[source] = struct{}{}
			workspaces = append(workspaces, source)
		}
		directMounts = append(directMounts, mount.Mount{Type: mount.TypeBind, Source: source, Target: target})
	}
	return normalized, bootstrap, workspaces, directMounts, nil
}

func (r *SbxRuntime) startRelays() ([]*TunnelRelay, []TunnelSpec, error) {
	host, err := r.tunnelHost()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sbx tunnel host: %w", err)
	}
	ports := append([]int{r.cfg.EnvdPort}, r.cfg.PublishedPorts...)
	ports = normalizeSbxPorts(ports)
	relays := make([]*TunnelRelay, 0, len(ports))
	specs := make([]TunnelSpec, 0, len(ports))
	for _, targetPort := range ports {
		token, err := newTunnelToken()
		if err != nil {
			closeTunnelRelays(relays)
			return nil, nil, err
		}
		relay, err := r.newRelay(token)
		if err != nil {
			closeTunnelRelays(relays)
			return nil, nil, err
		}
		relays = append(relays, relay)
		specs = append(specs, TunnelSpec{
			RelayAddress: net.JoinHostPort(host, strconv.Itoa(relay.Port())),
			Token:        token,
			TargetPort:   targetPort,
			PublicPort:   relay.Port(),
			Connections:  r.cfg.TunnelConnections,
		})
	}
	return relays, specs, nil
}

func (r *SbxRuntime) newRelay(token string) (*TunnelRelay, error) {
	if len(r.cfg.TunnelPortRange) == 0 {
		return NewTunnelRelay(r.cfg.TunnelBindHost, 0, token)
	}
	var lastErr error
	for port := r.cfg.TunnelPortRange[0]; port <= r.cfg.TunnelPortRange[1]; port++ {
		relay, err := NewTunnelRelay(r.cfg.TunnelBindHost, port, token)
		if err == nil {
			return relay, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("allocate sbx tunnel port: %w", lastErr)
}

func (r *SbxRuntime) runtimeEnvironment(req SandboxRuntimeCreateRequest, template SbxTemplateConfig, mode sbxMode, volumeMounts []VolumeMount, bootstrapVolumes []sbxVolumeSpec, tunnels []TunnelSpec) (map[string]string, error) {
	volumeData, err := json.Marshal(volumeMounts)
	if err != nil {
		return nil, err
	}
	bootstrapData, err := json.Marshal(bootstrapVolumes)
	if err != nil {
		return nil, err
	}
	tunnelData, err := json.Marshal(tunnels)
	if err != nil {
		return nil, err
	}
	metadataData, err := json.Marshal(gateway.CopyStringMap(req.Metadata))
	if err != nil {
		return nil, err
	}
	env := gateway.CopyStringMap(req.EnvVars)
	if env == nil {
		env = map[string]string{}
	}
	env[sbxManagedEnv] = "true"
	env[sbxModeEnv] = string(mode)
	env[sbxSandboxIDEnv] = req.SandboxID
	env[sbxTemplateIDEnv] = req.TemplateID
	env[sbxMetadataEnv] = string(metadataData)
	env[sbxCreatedAtEnv] = req.CreatedAt.UTC().Format(time.RFC3339Nano)
	env[sbxEndAtEnv] = req.EndAt.UTC().Format(time.RFC3339Nano)
	env[sbxVolumeMountsEnv] = string(volumeData)
	env[sbxBootstrapVolumesEnv] = string(bootstrapData)
	env[sbxTunnelsEnv] = string(tunnelData)
	env[sbxEnvdPortEnv] = strconv.Itoa(r.cfg.EnvdPort)
	if req.AllowInternetAccess != nil {
		env[sbxAllowInternetEnv] = strconv.FormatBool(*req.AllowInternetAccess)
	}
	if command := strings.TrimSpace(template.StartCmd); command != "" {
		env[sbxStartCommandEnv] = command
	}
	return env, nil
}

func (r *SbxRuntime) waitForSandboxContainer(ctx context.Context, name string) (dockertypes.Container, error) {
	deadline := r.now().Add(15 * time.Second)
	for {
		candidate, found, err := r.findSandboxContainer(ctx, name)
		if err != nil {
			return dockertypes.Container{}, err
		}
		if found {
			return candidate, nil
		}
		if r.now().After(deadline) {
			return dockertypes.Container{}, fmt.Errorf("sandboxd created %q but no matching docker container appeared", name)
		}
		if !waitForContext(ctx, 200*time.Millisecond) {
			return dockertypes.Container{}, ctx.Err()
		}
	}
}

func (r *SbxRuntime) findSandboxContainer(ctx context.Context, name string) (dockertypes.Container, bool, error) {
	containers, err := r.dockerClient.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return dockertypes.Container{}, false, fmt.Errorf("list sbx containers: %w", err)
	}
	for _, candidate := range containers {
		if candidate.Labels["com.docker.sandbox.name"] == name || containerHasName(candidate, name) {
			return candidate, true, nil
		}
	}
	return dockertypes.Container{}, false, nil
}

func (r *SbxRuntime) waitForContainerRunning(ctx context.Context, containerID string) error {
	return r.waitForContainerState(ctx, containerID, true)
}

func (r *SbxRuntime) waitForContainerStopped(ctx context.Context, containerID string) error {
	return r.waitForContainerState(ctx, containerID, false)
}

func (r *SbxRuntime) waitForContainerState(ctx context.Context, containerID string, running bool) error {
	for {
		inspect, err := r.dockerClient.ContainerInspect(ctx, containerID)
		if err != nil {
			return fmt.Errorf("inspect sbx container state: %w", err)
		}
		if inspect.State != nil && inspect.State.Running == running {
			return nil
		}
		if !waitForContext(ctx, 200*time.Millisecond) {
			return ctx.Err()
		}
	}
}

// sandboxd selects its own keep-alive command for agent sandboxes, so a
// custom template cannot rely on its OCI entrypoint. The image preloads the
// static helper and docker-next starts it detached after sandboxd owns the VM.
func (r *SbxRuntime) startGuestServices(ctx context.Context, containerID string, env map[string]string) error {
	exec, err := r.dockerClient.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Detach: true,
		Env:    environmentList(env),
		Cmd:    []string{"/usr/local/bin/sbx-init"},
	})
	if err != nil {
		return fmt.Errorf("create sbx guest bootstrap exec: %w", err)
	}
	if err := r.dockerClient.ContainerExecStart(ctx, exec.ID, container.ExecStartOptions{Detach: true}); err != nil {
		return fmt.Errorf("start sbx guest bootstrap exec: %w", err)
	}
	return nil
}

func (r *SbxRuntime) inspectRuntimeInfo(ctx context.Context, info SandboxRuntimeInfo) (SandboxRuntimeInfo, error) {
	inspect, err := r.dockerClient.ContainerInspect(ctx, info.ContainerID)
	if err != nil {
		return SandboxRuntimeInfo{}, fmt.Errorf("inspect sbx container: %w", err)
	}
	return r.runtimeInfoFromContainer(info, inspect)
}

func (r *SbxRuntime) runtimeInfoFromContainer(info SandboxRuntimeInfo, inspect dockertypes.ContainerJSON) (SandboxRuntimeInfo, error) {
	if inspect.ID != "" {
		info.ContainerID = inspect.ID
	}
	if info.ContainerName == "" {
		info.ContainerName = strings.TrimPrefix(inspect.Name, "/")
	}
	var environment []string
	if inspect.Config != nil {
		environment = inspect.Config.Env
	}
	env := environmentMap(environment)
	if info.SandboxID == "" {
		info.SandboxID = env[sbxSandboxIDEnv]
	}
	if info.MachineID == "" && env[sbxModeEnv] == string(sbxModeSandboxd) {
		info.MachineID = info.ContainerName
	}
	if mounts, err := volumeMountsFromEnvironment(env); err == nil && len(mounts) > 0 {
		info.VolumeMounts = mounts
	} else if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	tunnels, err := tunnelSpecsFromEnvironment(env)
	if err != nil {
		return SandboxRuntimeInfo{}, err
	}
	info = r.runtimeInfoWithTunnels(info, tunnels)
	if inspect.NetworkSettings != nil {
		info.ContainerIP = strings.TrimSpace(inspect.NetworkSettings.IPAddress)
	}
	return info, nil
}

func (r *SbxRuntime) runtimeInfoWithTunnels(info SandboxRuntimeInfo, tunnels []TunnelSpec) SandboxRuntimeInfo {
	if len(tunnels) == 0 {
		return info
	}
	info.HostPort = ""
	info.EnvdURL = ""
	info.PublishedPorts = nil
	for _, tunnel := range tunnels {
		if tunnel.TargetPort == r.cfg.EnvdPort {
			info.HostPort = strconv.Itoa(tunnel.PublicPort)
			info.EnvdURL = "http://" + net.JoinHostPort(r.cfg.TunnelPublicHost, info.HostPort)
			continue
		}
		info.PublishedPorts = append(info.PublishedPorts, gateway.SandboxPortMapping{
			ContainerPort: tunnel.TargetPort,
			HostIP:        r.cfg.TunnelPublicHost,
			HostPort:      tunnel.PublicPort,
			Protocol:      "tcp",
		})
	}
	sort.Slice(info.PublishedPorts, func(i, j int) bool {
		return info.PublishedPorts[i].ContainerPort < info.PublishedPorts[j].ContainerPort
	})
	return info
}

func (r *SbxRuntime) restoreSandboxRecord(inspect dockertypes.ContainerJSON, env map[string]string) (SandboxRecord, bool, error) {
	sandboxID := strings.TrimSpace(env[sbxSandboxIDEnv])
	if sandboxID == "" {
		return SandboxRecord{}, false, nil
	}
	info, err := r.runtimeInfoFromContainer(SandboxRuntimeInfo{
		SandboxID:     sandboxID,
		ContainerID:   inspect.ID,
		ContainerName: strings.TrimPrefix(inspect.Name, "/"),
	}, inspect)
	if err != nil {
		return SandboxRecord{}, false, err
	}
	metadata := map[string]string{}
	if raw := strings.TrimSpace(env[sbxMetadataEnv]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			return SandboxRecord{}, false, fmt.Errorf("decode restored sbx metadata: %w", err)
		}
	}
	createdAt := parseSbxTime(env[sbxCreatedAtEnv], r.now().UTC())
	endAt := parseSbxTime(env[sbxEndAtEnv], createdAt.Add(time.Duration(defaultSandboxTimeout)*time.Second))
	allowInternet := parseSbxBoolPtr(env[sbxAllowInternetEnv])
	state := sbxContainerState(inspect.State)
	if state == "" {
		return SandboxRecord{}, false, nil
	}
	return SandboxRecord{
		ID:                  sandboxID,
		TemplateID:          env[sbxTemplateIDEnv],
		Metadata:            metadata,
		EnvdURL:             info.EnvdURL,
		RuntimeInfo:         info,
		CreatedAt:           createdAt,
		EndAt:               endAt,
		State:               state,
		AllowInternetAccess: allowInternet,
	}, true, nil
}

func (r *SbxRuntime) restoreRelays(containerID string, env map[string]string) error {
	tunnels, err := tunnelSpecsFromEnvironment(env)
	if err != nil {
		return err
	}
	return r.restoreRelaysForTunnels(containerID, tunnels)
}

func (r *SbxRuntime) restoreRelaysForTunnels(containerID string, tunnels []TunnelSpec) error {
	if len(tunnels) == 0 {
		return nil
	}
	r.relayMu.Lock()
	if len(r.relays[containerID]) > 0 {
		r.relayMu.Unlock()
		return nil
	}
	r.relayMu.Unlock()
	relays := make([]*TunnelRelay, 0, len(tunnels))
	for _, tunnel := range tunnels {
		relay, err := NewTunnelRelay(r.cfg.TunnelBindHost, tunnel.PublicPort, tunnel.Token)
		if err != nil {
			closeTunnelRelays(relays)
			return fmt.Errorf("restore sbx tunnel relay on port %d: %w", tunnel.PublicPort, err)
		}
		relays = append(relays, relay)
	}
	r.registerRelays(containerID, relays)
	return nil
}

func (r *SbxRuntime) restoreSandboxdStates(ctx context.Context) ([]SandboxRecord, error) {
	states, err := r.listRuntimeStates()
	if err != nil {
		return nil, err
	}
	records := make([]SandboxRecord, 0, len(states))
	for _, state := range states {
		if state.Mode != sbxModeSandboxd {
			continue
		}
		container, found, err := r.findSandboxContainer(ctx, state.RuntimeInfo.ContainerName)
		if err != nil {
			return nil, err
		}
		if !found {
			if err := r.removeRuntimeState(state.SandboxID); err != nil {
				return nil, err
			}
			continue
		}
		inspection, err := r.InspectSandbox(ctx, mergeSbxRuntimeInfo(SandboxRuntimeInfo{ContainerID: container.ID, ContainerName: state.RuntimeInfo.ContainerName}, state.RuntimeInfo))
		if err != nil {
			return nil, err
		}
		if !inspection.Exists {
			continue
		}
		info := r.runtimeInfoWithTunnels(mergeSbxRuntimeInfo(inspection.Info, state.RuntimeInfo), state.Tunnels)
		if inspection.State == string(e2bapi.Running) {
			if err := r.restoreRelaysForTunnels(info.ContainerID, state.Tunnels); err != nil {
				return nil, err
			}
		}
		state.RuntimeInfo = info
		if err := r.saveRuntimeState(state); err != nil {
			return nil, err
		}
		records = append(records, state.record(info, inspection.State))
	}
	return records, nil
}

func mergeSbxRuntimeInfo(primary, fallback SandboxRuntimeInfo) SandboxRuntimeInfo {
	if primary.SandboxID == "" {
		primary.SandboxID = fallback.SandboxID
	}
	if primary.EnvdURL == "" {
		primary.EnvdURL = fallback.EnvdURL
	}
	if primary.ContainerID == "" {
		primary.ContainerID = fallback.ContainerID
	}
	if primary.ContainerName == "" {
		primary.ContainerName = fallback.ContainerName
	}
	if primary.ContainerIP == "" {
		primary.ContainerIP = fallback.ContainerIP
	}
	if primary.HostPort == "" {
		primary.HostPort = fallback.HostPort
	}
	if primary.MachineID == "" {
		primary.MachineID = fallback.MachineID
	}
	if len(primary.VolumeMounts) == 0 {
		primary.VolumeMounts = append([]VolumeMount(nil), fallback.VolumeMounts...)
	}
	if len(primary.PublishedPorts) == 0 {
		primary.PublishedPorts = append([]gateway.SandboxPortMapping(nil), fallback.PublishedPorts...)
	}
	return primary
}

func (r *SbxRuntime) runSandboxCommand(ctx context.Context, info SandboxRuntimeInfo, command []string) (string, error) {
	if r.isSandboxdManaged(info) {
		result, err := r.sandboxdClient.Exec(ctx, info.ContainerName, command)
		if err == nil {
			if result.ExitCode != 0 {
				return "", fmt.Errorf("sandboxd exec exited %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr))
			}
			return strings.TrimSpace(strings.Join(nonEmptyStrings(result.Stdout, result.Stderr), "\n")), nil
		}
		return "", fmt.Errorf("exec through sandboxd: %w", err)
	}
	exec, err := r.dockerClient.ContainerExecCreate(ctx, info.ContainerID, container.ExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          command,
	})
	if err != nil {
		return "", fmt.Errorf("create sbx docker exec: %w", err)
	}
	attached, err := r.dockerClient.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", fmt.Errorf("attach sbx docker exec: %w", err)
	}
	defer attached.Close()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attached.Reader); err != nil {
		return "", fmt.Errorf("read sbx docker exec: %w", err)
	}
	inspect, err := r.dockerClient.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return "", fmt.Errorf("inspect sbx docker exec: %w", err)
	}
	if inspect.ExitCode != 0 {
		return "", fmt.Errorf("sbx docker exec exited %d: %s", inspect.ExitCode, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(strings.Join(nonEmptyStrings(stdout.String(), stderr.String()), "\n")), nil
}

func (r *SbxRuntime) removeContainer(ctx context.Context, containerID string) error {
	if strings.TrimSpace(containerID) == "" {
		return nil
	}
	if err := r.dockerClient.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove sbx container: %w", err)
	}
	return nil
}

func (r *SbxRuntime) defaultHealthCheck(ctx context.Context, envdURL string) error {
	deadline := r.now().Add(time.Duration(r.cfg.HealthTimeoutSeconds) * time.Second)
	healthURL := strings.TrimRight(envdURL, "/") + "/health"
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return fmt.Errorf("create sbx envd health request: %w", err)
		}
		response, err := r.httpClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
				return nil
			}
		}
		if r.now().After(deadline) {
			return fmt.Errorf("envd did not become healthy at %s within %ds", healthURL, r.cfg.HealthTimeoutSeconds)
		}
		if !waitForContext(ctx, 250*time.Millisecond) {
			return ctx.Err()
		}
	}
}

func (r *SbxRuntime) registerRelays(containerID string, relays []*TunnelRelay) {
	if containerID == "" || len(relays) == 0 {
		return
	}
	r.relayMu.Lock()
	r.relays[containerID] = append(r.relays[containerID], relays...)
	r.relayMu.Unlock()
}

func (r *SbxRuntime) closeRelays(containerID string) {
	r.relayMu.Lock()
	relays := r.relays[containerID]
	delete(r.relays, containerID)
	r.relayMu.Unlock()
	closeTunnelRelays(relays)
}

func (r *SbxRuntime) moveRelays(previousContainerID, currentContainerID string) {
	if previousContainerID == "" || currentContainerID == "" || previousContainerID == currentContainerID {
		return
	}
	r.relayMu.Lock()
	previous := r.relays[previousContainerID]
	if len(previous) == 0 {
		r.relayMu.Unlock()
		return
	}
	if len(r.relays[currentContainerID]) == 0 {
		r.relays[currentContainerID] = previous
		delete(r.relays, previousContainerID)
		r.relayMu.Unlock()
		return
	}
	delete(r.relays, previousContainerID)
	r.relayMu.Unlock()
	closeTunnelRelays(previous)
}

func (r *SbxRuntime) isSandboxdManaged(info SandboxRuntimeInfo) bool {
	return strings.TrimSpace(info.MachineID) != ""
}

func closeTunnelRelays(relays []*TunnelRelay) {
	for _, relay := range relays {
		if relay != nil {
			_ = relay.Close()
		}
	}
}

func environmentList(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func environmentMap(values []string) map[string]string {
	result := map[string]string{}
	for _, value := range values {
		key, value, ok := strings.Cut(value, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}

func volumeMountsFromEnvironment(env map[string]string) ([]VolumeMount, error) {
	raw := strings.TrimSpace(env[sbxVolumeMountsEnv])
	if raw == "" {
		return nil, nil
	}
	var mounts []VolumeMount
	if err := json.Unmarshal([]byte(raw), &mounts); err != nil {
		return nil, fmt.Errorf("decode sbx volume mounts: %w", err)
	}
	return mounts, nil
}

func tunnelSpecsFromEnvironment(env map[string]string) ([]TunnelSpec, error) {
	raw := strings.TrimSpace(env[sbxTunnelsEnv])
	if raw == "" {
		return nil, nil
	}
	var tunnels []TunnelSpec
	if err := json.Unmarshal([]byte(raw), &tunnels); err != nil {
		return nil, fmt.Errorf("decode sbx tunnels: %w", err)
	}
	return tunnels, nil
}

func sbxContainerState(state *dockertypes.ContainerState) string {
	if state == nil {
		return ""
	}
	if state.Running {
		return string(e2bapi.Running)
	}
	if state.Paused || state.Status == "exited" || state.Status == "created" {
		return string(e2bapi.Paused)
	}
	return ""
}

func containerHasName(info dockertypes.Container, name string) bool {
	for _, candidate := range info.Names {
		if strings.TrimPrefix(candidate, "/") == name {
			return true
		}
	}
	return false
}

func normalizeSbxPorts(ports []int) []int {
	seen := map[int]struct{}{}
	result := make([]int, 0, len(ports))
	for _, port := range ports {
		if port <= 0 || port > 65535 {
			continue
		}
		if _, ok := seen[port]; ok {
			continue
		}
		seen[port] = struct{}{}
		result = append(result, port)
	}
	sort.Ints(result)
	return result
}

func shortSbxImageID(id string) string {
	id = strings.TrimPrefix(strings.TrimSpace(id), "sha256:")
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func bytesToMB(value int64) int {
	if value <= 0 {
		return 0
	}
	return int((value + 1024*1024 - 1) / (1024 * 1024))
}

func memoryMB(value string) int {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0
	}
	multiplier := 1
	if strings.HasSuffix(value, "GB") || strings.HasSuffix(value, "G") {
		multiplier = 1024
		value = strings.TrimSuffix(strings.TrimSuffix(value, "GB"), "G")
	} else if strings.HasSuffix(value, "MB") || strings.HasSuffix(value, "M") {
		value = strings.TrimSuffix(strings.TrimSuffix(value, "MB"), "M")
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed * multiplier
}

func safeSbxImageComponent(value string) string {
	var result strings.Builder
	lastDash := false
	for _, character := range strings.ToLower(strings.TrimSpace(value)) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			result.WriteRune(character)
			lastDash = false
			continue
		}
		if !lastDash {
			result.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(result.String(), "-")
}

func parseSbxTime(value string, fallback time.Time) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed.UTC()
}

func parseSbxBoolPtr(value string) *bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return nil
	}
	return &parsed
}

func nonEmptyStrings(values ...string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func sbxLogEntries(output string, req SandboxLogsRequest, now time.Time) []SandboxRuntimeLogEntry {
	lines := strings.Split(output, "\n")
	entries := make([]SandboxRuntimeLogEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		timestamp := now.UTC()
		message := line
		if separator := strings.IndexByte(line, ' '); separator > 0 {
			if parsed, err := time.Parse(time.RFC3339Nano, line[:separator]); err == nil {
				timestamp = parsed.UTC()
				message = line[separator+1:]
			}
		}
		if req.Start != nil && timestamp.UnixMilli() < *req.Start {
			continue
		}
		if req.Cursor != nil && timestamp.UnixMilli() < *req.Cursor {
			continue
		}
		if req.Search != nil && !strings.Contains(message, *req.Search) {
			continue
		}
		entries = append(entries, SandboxRuntimeLogEntry{Timestamp: timestamp, Level: e2bapi.LogLevelInfo, Message: message, Fields: map[string]string{"source": "sbx"}})
	}
	if req.Direction != nil && *req.Direction == e2bapi.LogsDirectionBackward {
		for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
			entries[left], entries[right] = entries[right], entries[left]
		}
	}
	if req.Limit > 0 && len(entries) > int(req.Limit) {
		entries = entries[:req.Limit]
	}
	return entries
}
