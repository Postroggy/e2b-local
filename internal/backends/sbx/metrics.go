package sbxbackend

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var metricsWorkingDirectoryMu sync.Mutex

type metricsAPI interface {
	Read(ctx context.Context, containerID string) (map[string]float64, error)
}

type metricsClient struct {
	root string
}

func newMetricsClient(root string) *metricsClient {
	return &metricsClient{root: strings.TrimSpace(root)}
}

func (c *metricsClient) Read(ctx context.Context, containerID string) (map[string]float64, error) {
	containerID = strings.TrimSpace(containerID)
	if containerID == "" {
		return nil, fmt.Errorf("container id is required for metrics")
	}
	if c.root == "" {
		return nil, fmt.Errorf("sbx.metrics_root is required for metrics")
	}

	socketPath := filepath.Join(c.root, containerID, "vm", "metrics.sock")
	connection, err := dialLongUnixSocket(ctx, socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect metrics socket: %w", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://metrics/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("create metrics request: %w", err)
	}
	if err := request.Write(connection); err != nil {
		return nil, fmt.Errorf("write metrics request: %w", err)
	}

	response, err := http.ReadResponse(bufio.NewReader(connection), request)
	if err != nil {
		return nil, fmt.Errorf("read metrics response: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return nil, fmt.Errorf("metrics returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(data)))
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read metrics payload: %w", err)
	}
	return parsePrometheusMetrics(string(data)), nil
}

// Sailor's per-VM metrics socket path exceeds the Unix sockaddr pathname
// limit. Dial it from its containing directory with a short relative name.
func dialLongUnixSocket(ctx context.Context, socketPath string) (net.Conn, error) {
	directory := filepath.Dir(socketPath)
	socketName := filepath.Base(socketPath)

	metricsWorkingDirectoryMu.Lock()
	defer metricsWorkingDirectoryMu.Unlock()

	currentDirectory, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	if err := os.Chdir(directory); err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	connection, dialErr := dialer.DialContext(ctx, "unix", socketName)
	restoreErr := os.Chdir(currentDirectory)
	if restoreErr != nil {
		if connection != nil {
			_ = connection.Close()
		}
		if dialErr != nil {
			return nil, fmt.Errorf("dial metrics socket: %w; restore working directory: %v", dialErr, restoreErr)
		}
		return nil, fmt.Errorf("restore working directory: %w", restoreErr)
	}
	if dialErr != nil {
		return nil, dialErr
	}
	return connection, nil
}

func parsePrometheusMetrics(payload string) map[string]float64 {
	values := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(payload))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if index := strings.IndexByte(name, '{'); index >= 0 {
			name = name[:index]
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		values[name] += value
	}
	return values
}

func metricValueBySuffix(values map[string]float64, suffixes ...string) float64 {
	for _, suffix := range suffixes {
		for name, value := range values {
			if strings.HasSuffix(name, suffix) {
				return value
			}
		}
	}
	return 0
}
