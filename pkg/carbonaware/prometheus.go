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
	// nodeMeasureQueryTemplate = "sum_over_time(node_network_receive_bytes_total{kubernetes_node=\"%s\",device=\"%s\"}[%s])"
	// nodeTotalConsumptionQuery = "sum_over_time(kepler_container_joules_total{instance=\"%s\"}[%s])"
    // nodeCoreEnergyQueryTemplate  = "kepler_node_core_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	// nodeDramEnergyQueryTemplate  = "kepler_node_dram_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	// nodeUnCoreEnergyQueryTemplate = "kepler_node_uncore_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	// nodeOtherEnergyQueryTemplate  = "kepler_node_other_joules_total{instance=\"%s\",mode=\"dynamic\"}[%s]"
	hostPowerQuery = "irate(kepler_node_platform_joules_total{instance=\"%s\"}[1m])"
	//containerEnergyQueryTemplate = "irate(kepler_container_joules_total{pod_name=~\".*%s.*\"}[1m])"
	containerEnergyQueryTemplate = "max(max_over_time(irate(kepler_container_joules_total{pod_name=~\".*%s.*\"}[1m])[1d:]))"
 


	WATT_TO_MICROWATT = 1e+6
	timeElapsed = 60.0
)


// Handles the interaction of the networkplugin with Prometheus
type PrometheusHandle struct {
	//timeRange        time.Duration
	address          string
	api              v1.API
}

func NewPrometheus(address string) *PrometheusHandle {
	client, err := api.NewClient(api.Config{
		Address: address,
	})
	if err != nil {
		klog.Fatalf("[CarbonAware] Error creating prometheus client: %s", err.Error())
	}

	return &PrometheusHandle{
		//timeRange:        timeRange,
		address:          address,
		api:              v1.NewAPI(client),
	}
}


func (p *PrometheusHandle) query(query_str,node string) (model.Value, error) {

	queryString := fmt.Sprintf(query_str, node)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	results, warnings, err := p.api.Query(ctx, queryString, time.Now())
	cancel()

	if len(warnings) > 0 {
		klog.Warningf("[CarbonAware] Warnings: %v\n", warnings)
	}

	return results, err
}

func (p *PrometheusHandle) getPodTotalPower(podBaseName string) (int64, error) {
	res, err := p.query(containerEnergyQueryTemplate, podBaseName)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error querying prometheus: %w", err)
	}
	podMeasure := res.(model.Vector)
	if len(podMeasure) == 0 {
		return 0, fmt.Errorf("[CarbonAware] Invalid response, expected 2 value, got %d", len(podMeasure))
	}
	// Get the power value
	
	power := WATT_TO_MICROWATT*float64(podMeasure[0].Value)/timeElapsed

	return int64(power), nil
}

func (p *PrometheusHandle) getNodePowerMeasure(node string) (int64, error) {
	res, err := p.query(hostPowerQuery,node)
	//res, err := p.query(query)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error querying prometheus: %w", err)
	}
	nodeMeasure := res.(model.Vector)
	if len(nodeMeasure) == 0 {
		return 0, fmt.Errorf("[CarbonAware] Invalid response, expected 2 value, got %d", len(nodeMeasure))
	}
	// Get the power value
	power := 0.0
	for _, sample := range nodeMeasure {
		power = WATT_TO_MICROWATT*float64(sample.Value)/timeElapsed

	}


	return int64(power), nil
}
