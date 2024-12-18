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
	hostPowerQuery = "irate(kepler_node_platform_joules_total{instance=\"%s\"}[1m])"
	//containerEnergyQueryTemplate = "irate(kepler_container_joules_total{pod_name=~\".*%s.*\"}[1m])"
	//containerEnergyQueryTemplate = "max(max_over_time(irate(kepler_container_joules_total{pod_name=~\".*%s.*\"}[1m])[1d:]))"
	containerEnergyQueryTemplate = `%s(%s(%s(kepler_container_joules_total{pod_name=~".*%s.*"}[%s])[%s:]))`


	WATT_TO_MICROWATT = 1000000.0
	timeElapsed = 60.0
)

type PrometheusConfig struct {
	AggregationFunction string
	RangeFunction string
	RateFunction string
	RateDuration string
	QueryRange string
}
// Handles the interaction of the networkplugin with Prometheus
type PrometheusHandle struct {
	//timeRange        time.Duration
	//address          string
	api              v1.API
	config 		 PrometheusConfig
}

func generateQuery(config PrometheusConfig, podName string) string {
	return fmt.Sprintf(containerEnergyQueryTemplate, 
		config.AggregationFunction,
		config.RangeFunction, 
		config.RateFunction,
		podName, 
		config.RateDuration, 
		config.QueryRange)
}

func NewPrometheus(address string, config PrometheusConfig) *PrometheusHandle {
	client, err := api.NewClient(api.Config{
		Address: address,
	})
	if err != nil {
		klog.Fatalf("[CarbonAware] Error creating prometheus client: %s", err.Error())
	}

	return &PrometheusHandle{
		api:              v1.NewAPI(client),
		config: config,
	}
}


func (p *PrometheusHandle) query(queryString string) (model.Value, error) {

	//queryString := fmt.Sprintf(query_str, obj_name)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	results, warnings, err := p.api.Query(ctx, queryString, time.Now())
	cancel()

	if len(warnings) > 0 {
		klog.Warningf("[CarbonAware] Warnings: %v\n", warnings)
	}

	return results, err
}

func (p *PrometheusHandle) getPodTotalPower(podBaseName string) (int64, error) {
	queryString := generateQuery(p.config, podBaseName)
	res, err := p.query(queryString)
	if err != nil {
		return 0, fmt.Errorf("[CarbonAware] Error querying prometheus: %w", err)
	}
	podMeasure := res.(model.Vector)
	if len(podMeasure) == 0 {
		return 0, fmt.Errorf("[CarbonAware] Invalid response, got %d", len(podMeasure))
	}
	// Get the power value
	
	power := WATT_TO_MICROWATT*float64(podMeasure[0].Value)/timeElapsed

	return int64(power), nil
}

func (p *PrometheusHandle) getNodePowerMeasure(node string) (int64, error) {
	queryString := fmt.Sprintf(hostPowerQuery, node)
	res, err := p.query(queryString)
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
