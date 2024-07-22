package carbonaware

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/klog/v2"
)

const (
	// nodeMeasureQueryTemplate is the template string to get the query for the node used bandwidth
	nodeMeasureQueryTemplate = "sum_over_time(node_network_receive_bytes_total{kubernetes_node=\"%s\",device=\"%s\"}[%s])"
	nodeTotalConsumptionQuery = "sum_over_time(kepler_container_joules_total{kubernetes_node=\"%s\"}[%s])"
    nodeCoreEnergyQueryTemplate = "kepler_node_core_joules_total{kubernetes_node=\"%s\"}[%s]"
	nodeDramEnergyQueryTemplate = "keple_node_dram_joules_total{kubernetes_node=\"%s\"}[%s]"
	nodeUnCoreEnergyQueryTemplate = "kepler_node_uncore_joules_total{kubernetes_node=\"%s\"}[%s]"
	nodeOtherEnergyQueryTemplate = "kepler_node_other_joules_total{kubernetes_node=\"%s\"}[%s]"
)

// Handles the interaction of the networkplugin with Prometheus
type PrometheusHandle struct {
	timeRange        time.Duration
	address          string
	api              v1.API
}

func NewPrometheus(address string, timeRange time.Duration) *PrometheusHandle {
	client, err := api.NewClient(api.Config{
		Address: address,
	})
	if err != nil {
		klog.Fatalf("[CarbonAware] Error creating prometheus client: %s", err.Error())
	}

	return &PrometheusHandle{
		timeRange:        timeRange,
		address:          address,
		api:              v1.NewAPI(client),
	}
}

func (p *PrometheusHandle) GetNodeEnergyMeasure(node string) (*model.Sample, error) {
	query := getNodeEnergyQuery(node, p.timeRange)
	res, err := p.query(query)
	if err != nil {
		return nil, fmt.Errorf("[CarbonAware] Error querying prometheus: %w", err)
	}

	nodeMeasure := res.(model.Vector)
	if len(nodeMeasure) != 1 {
		return nil, fmt.Errorf("[CarbonAware] Invalid response, expected 1 value, got %d", len(nodeMeasure))
	}

	return nodeMeasure[0], nil
}

func getNodeEnergyQuery(node,query_str string, timeRange time.Duration) string {
	return fmt.Sprintf(query_str, node, timeRange)
}

func (p *PrometheusHandle) query(query string) (model.Value, error) {
	results, warnings, err := p.api.Query(context.Background(), query, time.Now())

	if len(warnings) > 0 {
		klog.Warningf("[CarbonAware] Warnings: %v\n", warnings)
	}

	return results, err
}

