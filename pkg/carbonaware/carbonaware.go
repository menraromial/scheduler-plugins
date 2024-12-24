// Package carbonaware implements a Kubernetes scheduler plugin that considers
// power consumption and carbon awareness in scheduling decisions.
package carbonaware

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
)

// Configuration constants
const (
	Name = "CarbonAware"
	ErrReason = "node(s) didn't meet power requirements for the requested pod"
	
	// State keys
	preFilterStateKey = "PreFilter" + Name
	preScoreStateKey  = "PreScore" + Name

	// Label constants
	labelPrefix = "rapl"
	labelSuffix = "power_limit_uw"
	
)

// Prometheus configuration defaults and environment keys
const (
	prometheusURLDefault = "http://prometheus-k8s.monitoring.svc.cluster.local:9090"
	prometheusURLEnv     = "PROMETHEUS_URL"

	aggregationFuncDefault = "max"
	rangeFuncDefault      = "max_over_time"
	rateFuncDefault       = "irate"
	rateDurationDefault   = "1m"
	queryRangeDefault     = "1d"

	aggregationFuncEnv = "AGGREGATION_FUNCTION"
	rangeFuncEnv      = "RANGE_FUNCTION"
	rateFuncEnv       = "RATE_FUNCTION"
	rateDurationEnv   = "RATE_DURATION"
	queryRangeEnv     = "QUERY_RANGE"
)

// Verify interface implementations
var (
	_ framework.PreFilterPlugin = &CarbonAware{}
	_ framework.FilterPlugin    = &CarbonAware{}
	_ framework.PreScorePlugin  = &CarbonAware{}
	_ framework.ScorePlugin     = &CarbonAware{}
)

// CarbonAware implements the scheduler plugin interface
type CarbonAware struct {
	handle     framework.Handle
	prometheus *PrometheusHandle
}

// NodeResources holds resource information for a node
type NodeResources struct {
	MinPowerLimit int64 // Minimum power limit > 0 across all RAPL domains
    MaxPowerLimit int64 // Maximum power limit across all RAPL domains
}

// preFilterState represents the state computed during PreFilter
type preFilterState struct {
	res           framework.Resource
	nodeResources map[string]NodeResources
	podBaseName   string // app.kcas/name
}

// Clone implements the StateData interface
func (s *preFilterState) Clone() framework.StateData {
	return s
}

// preScoreState represents the state computed during PreScore
type preScoreState struct {
	podInfo       framework.Resource
	nodeResources map[string]NodeResources
	podBaseName   string // app.kcas/name
}

// Clone implements the StateData interface
func (s *preScoreState) Clone() framework.StateData {
	return s
}

// New creates a new CarbonAware plugin instance
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.CarbonAwareArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type CarbonAwareArgs, got %T", obj)
	}

	prometheusConfig := PrometheusConfig{
		AggregationFunction: getEnvOrDefault(aggregationFuncEnv, aggregationFuncDefault),
		RangeFunction:      getEnvOrDefault(rangeFuncEnv, rangeFuncDefault),
		RateFunction:       getEnvOrDefault(rateFuncEnv, rateFuncDefault),
		RateDuration:       getEnvOrDefault(rateDurationEnv, rateDurationDefault),
		QueryRange:         getEnvOrDefault(queryRangeEnv, queryRangeDefault),
	}

	prometheusURL := getEnvOrDefault(prometheusURLEnv, prometheusURLDefault)
	klog.Info("Initializing CarbonAware plugin ", 
		" timeRange ", args.TimeRangeInMinutes,
		" address ", args.Address,
		" prometheusConfig ", prometheusConfig)

	return &CarbonAware{
		handle:     handle,
		prometheus: NewPrometheus(prometheusURL, prometheusConfig),
	}, nil
}

// Plugin interface methods
func (ca *CarbonAware) Name() string { return Name }
func (ca *CarbonAware) ScoreExtensions() framework.ScoreExtensions { return ca }

// PreFilter computes the initial state used by Filter and Score
func (ca *CarbonAware) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	nodeRes, err := ca.computeNodeResources()
	if err != nil {
		return nil, err
	}

	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})

	preFilterState := &preFilterState{
		//res:           resource.PodRequests(pod, resource.PodResourcesOptions{}),
		nodeResources: nodeRes,
		podBaseName:   pod.GetLabels()["app.kcas/name"],
	}

	preFilterState.res.SetMaxResource(reqs)

	state.Write(preFilterStateKey, preFilterState)
	return nil, nil
}

func (ca *CarbonAware) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// Filter checks if a node has sufficient power for the pod
func (ca *CarbonAware) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Filtering node ", "node ", nodeInfo.Node().Name, " pod ", pod.Name)

	preFilterState, err := getPreFilterState(state)
	if err != nil {
		return framework.AsStatus(err)
	}

	if !ca.fitsPower(preFilterState, nodeInfo) {
		return framework.NewStatus(framework.Unschedulable, ErrReason)
	}
	return nil
}

// PreScore prepares scoring data
func (ca *CarbonAware) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	preFilterState, err := getPreFilterState(state)
	if err != nil {
		return framework.AsStatus(err)
	}

	filteredNodeResources := make(map[string]NodeResources)
	for _, node := range nodes {
		if nodeRes, ok := preFilterState.nodeResources[node.Node().Name]; ok {
			filteredNodeResources[node.Node().Name] = nodeRes
		}
	}

	preScoreState := &preScoreState{
		podInfo:       preFilterState.res,
		nodeResources: filteredNodeResources,
		podBaseName:   preFilterState.podBaseName,
	}

	state.Write(preScoreStateKey, preScoreState)
	return nil
}

// Score assigns scores to nodes based on power availability
func (ca *CarbonAware) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	logger := klog.FromContext(ctx)
	
	nodeConsumption, err := ca.prometheus.getNodePowerMeasure(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting node power: %v", err))
	}

	preScoreState, err := getPreScoreState(state)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	nodeRes, ok := preScoreState.nodeResources[nodeName]
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %s not found in preScoreState", nodeName))
	}

	podPower, err := ca.prometheus.getPodTotalPower(preScoreState.podBaseName)
	if err != nil {
		logger.Error(err, " Error getting pod power")
	}

	availablePower := nodeRes.MaxPowerLimit - nodeConsumption
	score := availablePower - podPower

	logger.V(4).Info("Computed node score ", 
		" pod ", pod.Name,
		" node ", nodeName,
		" score ", score)

	return score, nil
}

// NormalizeScore normalizes the scores across all nodes
func (ca *CarbonAware) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Normalizing scores ", "before ", scores)

	minScore, maxScore := getMinMaxScores(scores)
	if minScore == 0 && maxScore == 0 {
		return nil
	}

	for i := range scores {
		if maxScore != minScore {
			normScore := float64(framework.MaxNodeScore) * float64(scores[i].Score) / float64(maxScore)
			scores[i].Score = int64(normScore)
		} else {
			scores[i].Score = framework.MaxNodeScore
		}
	}

	logger.V(4).Info("Normalized scores ", "after ", scores)
	return nil
}

// Helper functions

func (ca *CarbonAware) computeNodeResources() (map[string]NodeResources, *framework.Status) {
	nodeRes := make(map[string]NodeResources)
	
	nodeInfo, err := ca.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("error listing nodes: %v", err))
	}

	for _, node := range nodeInfo {
		nodeName := node.Node().Name
		
       // Récupérer min et max des power limits
	   minPower, maxPower, err := ca.getPowerLimitsFromLabels(nodeName)
	   if err != nil {
		   klog.ErrorS(err, " Failed to get power limits ", "node ", nodeName)
		   continue
	   }

		nodeRes[nodeName] = NodeResources{
			MinPowerLimit: minPower,
			MaxPowerLimit: maxPower,
		}
	}

	return nodeRes, nil
}



func (ca *CarbonAware) fitsPower(state *preFilterState, nodeInfo *framework.NodeInfo) bool {
	nodeName := nodeInfo.Node().Name
	
	nodeConsumption, err := ca.prometheus.getNodePowerMeasure(nodeName)
	if err != nil {
		klog.ErrorS(err, " Failed to get node power consumption ", "node ", nodeName)
		return false
	}

	podPower, err := ca.prometheus.getPodTotalPower(state.podBaseName)
	if err != nil {
		klog.ErrorS(err, " Failed to get pod power consumption ", "pod ", state.podBaseName)
		return false
	}

	nodeRes, ok := state.nodeResources[nodeName]
	if !ok {
		return false
	}

	availablePower := nodeRes.MaxPowerLimit - nodeConsumption

	klog.V(4).Info("Power fit check ",
		" node ", nodeName,
		" availablePower ", availablePower,
		" podPower ", podPower)

	return availablePower >= podPower
}

func getPreFilterState(state *framework.CycleState) (*preFilterState, error) {
	data, err := state.Read(preFilterStateKey)
	if err != nil {
		return nil, fmt.Errorf("error reading prefilter state: %w", err)
	}

	s, ok := data.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("invalid prefilter state type: %T", data)
	}
	return s, nil
}

func getPreScoreState(state *framework.CycleState) (*preScoreState, error) {
	data, err := state.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("error reading prescore state: %w", err)
	}

	s, ok := data.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid prescore state type: %T", data)
	}
	return s, nil
}

func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	if len(scores) == 0 {
		return 0, 0
	}

	min, max := scores[0].Score, scores[0].Score
	for _, score := range scores {
		if score.Score < min {
			min = score.Score
		}
		if score.Score > max {
			max = score.Score
		}
	}
	return min, max
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}



// getPowerLimitsFromLabels returns the min (>0) and max power limits from all matching labels
func (ca *CarbonAware) getPowerLimitsFromLabels(nodeName string) (min int64, max int64, err error) {
    node, err := ca.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
    if err != nil {
        return 0, 0, fmt.Errorf("failed to get node %s: %v", nodeName, err)
    }

    min = math.MaxInt64
    max = 0
    foundValidValue := false

    // Parcourir tous les labels du node
    for label, valueStr := range node.Node().Labels {
        // Vérifier si le label correspond au pattern
        if strings.HasPrefix(label, labelPrefix) && strings.HasSuffix(label, labelSuffix) {
            value, err := strconv.ParseInt(valueStr, 10, 64)
            if err != nil {
                klog.V(4).Info("Failed to parse power limit ", 
                    " node ", nodeName,
                    " label ", label,
                    " value ", valueStr,
                    " error ", err)
                continue
            }

            // Ne considérer que les valeurs strictement positives
            if value > 0 {
                foundValidValue = true
                if value < min {
                    min = value
                }
                if value > max {
                    max = value
                }
            }
        }
    }

    if !foundValidValue {
        return 0, 0, fmt.Errorf("no valid power limit values found for node %s", nodeName)
    }

    klog.V(4).Info("Found power limits ",
        " node ", nodeName,
        " min ", min,
        " max ", max)

    return min, max, nil
}