package carbonaware

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"time"
)

const (
	Name          = "CarbonAware"
	prometheusURL = "http://10.128.0.3:9090/api/v1/query?query=kepler_node_package_joules_total"
)

var _ framework.PreFilterPlugin = &CarbonAware{}
var _ framework.FilterPlugin = &CarbonAware{}
var _ framework.PreScorePlugin = &CarbonAware{}
var _ framework.ScorePlugin = &CarbonAware{}
//var _ framework.NodeScoreExtension = &CarbonAware{}

// EnergyMetrics holds the energy metrics for nodes
type EnergyMetrics struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric struct {
				Instance string `json:"instance"`
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

type CarbonAware struct {
	handle        framework.Handle
	energyMetrics map[string]float64
	prometheus    *PrometheusHandle
}



func (eas *CarbonAware) Name() string {
	return Name
}



func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	args, ok := obj.(*config.CarbonAwareArgs)
	if !ok {
		return nil, fmt.Errorf("[CarbonAware] want args to be of type CarbonAwareArgs, got %T", obj)
	}

	klog.Infof("[CarbonAware] args received.TimeRangeInMinutes: %d, Address: %s", args.TimeRangeInMinutes, args.Address)

	return &CarbonAware{
		handle:        handle,
		energyMetrics: fetchEnergyMetrics(),
		prometheus:    NewPrometheus(args.Address, time.Minute*time.Duration(args.TimeRangeInMinutes)),
	}, nil

}

func fetchEnergyMetrics() map[string]float64 {
	resp, err := http.Get(prometheusURL)
	if err != nil {
		log.Fatalf("Failed to fetch energy metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %v", err)
	}

	var metrics EnergyMetrics
	if err := json.Unmarshal(body, &metrics); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	energyMetrics := make(map[string]float64)
	for _, result := range metrics.Data.Result {
		instance := result.Metric.Instance
		value, _ := result.Value[1].(string)
		//energy, _ := strconv.ParseFloat(value, 64)
		energy, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Printf("Failed to parse energy value for instance %s: %v", instance, err)
			continue
		}
		energyMetrics[instance] = energy
	}
	return energyMetrics
}

/*
func main() {
	registry := registry.NewRegistry()
	registry.Register(pluginName, New)

	fmt.Printf("%s plugin registered.\n", pluginName)
}
*/


// PreFilter implements framework.PreFilterPlugin.
func (eas *CarbonAware) PreFilter(ctx context.Context, state *framework.CycleState, p *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	panic("unimplemented")
}

// Filter implements framework.FilterPlugin.
func (eas *CarbonAware) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	panic("unimplemented")
}

// PreScore implements framework.PreScorePlugin.
func (eas *CarbonAware) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	panic("unimplemented")
}

// Score implements framework.ScorePlugin.
func (eas *CarbonAware) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	panic("unimplemented")
}


// PreFilterExtensions implements framework.PreFilterPlugin.
func (eas *CarbonAware) PreFilterExtensions() framework.PreFilterExtensions {
	panic("unimplemented")
}



func (eas *CarbonAware) ScoreExtensions() framework.ScoreExtensions {
	return nil
}



