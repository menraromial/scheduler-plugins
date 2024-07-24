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
	nodeTotalConsumptionQuery = "sum_over_time(kepler_container_joules_total{instance=\"%s\"}[%s])"
    nodeCoreEnergyQueryTemplate  = "kepler_node_core_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	nodeDramEnergyQueryTemplate  = "kepler_node_dram_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	nodeUnCoreEnergyQueryTemplate = "kepler_node_uncore_joules_total{instance=\"%s\",mode=\"dynamic\"}[%sm]"
	nodeOtherEnergyQueryTemplate  = "kepler_node_other_joules_total{instance=\"%s\",mode=\"dynamic\"}[%s]"

	WH_TO_MICROWATT = 1000000
	timeElapsed = 30.0
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


func (p *PrometheusHandle) GetNodeEnergyMeasure(node,qr string) (model.Matrix, error) {
	query := getNodeEnergyQuery(qr,node, p.timeRange)
	res, err := p.query(query)
	if err != nil {
		return nil, fmt.Errorf("[CarbonAware] Error querying prometheus: %w", err)
	}
	nodeMeasure := res.(model.Matrix)
	if len(nodeMeasure[0].Values) != 2 {
		return nil, fmt.Errorf("[CarbonAware] Invalid response, expected 2 value, got %d", len(nodeMeasure))
	}
	return nodeMeasure, nil
}

func (p *PrometheusHandle) GetTotalConsumptionNodeEnergy(node string) (int64, error) {
	coreQuery := getNodeEnergyQuery(node, nodeCoreEnergyQueryTemplate, p.timeRange)
	dramQuery := getNodeEnergyQuery(node, nodeDramEnergyQueryTemplate, p.timeRange)
	uncoreQuery := getNodeEnergyQuery(node, nodeUnCoreEnergyQueryTemplate, p.timeRange)
	otherQuery := getNodeEnergyQuery(node, nodeOtherEnergyQueryTemplate, p.timeRange)

	coreEnergy, err := p.GetNodeEnergyMeasure(node, coreQuery)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error getting core energy: %w", err)
	}

	dramEnergy, err := p.GetNodeEnergyMeasure(node, dramQuery)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error getting dram energy: %w", err)
	}

	uncoreEnergy, err := p.GetNodeEnergyMeasure(node, uncoreQuery)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error getting uncore energy: %w", err)
	}

	otherEnergy, err := p.GetNodeEnergyMeasure(node, otherQuery)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error getting other energy: %w", err)
	}

	coreEnergyValue := float32(coreEnergy[0].Values[1].Value) - float32(coreEnergy[0].Values[0].Value)
	dramEnergyValue := float32(dramEnergy[0].Values[1].Value) - float32(dramEnergy[0].Values[0].Value)
	uncoreEnergyValue := float32(uncoreEnergy[0].Values[1].Value) - float32(uncoreEnergy[0].Values[0].Value)
	otherEnergyValue := float32(otherEnergy[0].Values[1].Value) - float32(otherEnergy[0].Values[0].Value)


	totalEnergy := (coreEnergyValue + dramEnergyValue + uncoreEnergyValue + otherEnergyValue)/timeElapsed*WH_TO_MICROWATT
	return int64(totalEnergy), nil
}

