package energyawarescore

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
)

const (
	Name = "EnergyAwareScorer"
	prometheusURL = "http://10.128.0.3:9090/api/v1/query?query=kepler_node_package_joules_total"
)

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

type EnergyAwareScorer struct {
	handle framework.Handle
	energyMetrics map[string]float64
}

func (eas *EnergyAwareScorer) Name() string {
	return Name
}

func (eas *EnergyAwareScorer) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	energy, exists := eas.energyMetrics[nodeName]
	if !exists {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("no energy data for node %s", nodeName))
	}
    log.Println("[EnergyAwareScorer] node '%s' energy: %s", nodeName, energy)

	// Invert the energy consumption to make lower energy consumption more favorable
	score := int64(1000000 / energy)
	return score, nil
}

func (eas *EnergyAwareScorer) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	eas := &EnergyAwareScorer{
		handle: handle,
		energyMetrics: fetchEnergyMetrics(),
	}
	return eas, nil
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