package carbonaware

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"

	//"time"

	//corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
)

const (
	Name = "CarbonAware"
	//prometheusURL = "http://10.128.0.3:9090/api/v1/query?query=kepler_node_package_joules_total"
	preFilterStateKey = "PreFilter" + Name
	preScoreStateKey  = "PreScore" + Name
	ErrReason                  = "node(s) didn't have power for the requested pod"
	constraints0PowerLimitStr  = "rapl0/constraint-1-power-limit-uw"
	constraints0PowerLimitCStr = "crapl0/constraint-1-power-limit-uw"

	//prometheusURL = "http://prometheus-server.default.svc.cluster.local"
	prometheusUrlDefault = "http://prometheus-k8s.monitoring.svc.cluster.local:9090"

	prometheusUrlEnv = "PROMETHEUS_URL"

	aggregationFunctionEnv = "AGGREGATION_FUNCTION"    
	rangeFunctionEnv = "RANGE_FUNCTION" 
	rateFunctionEnv = "RATE_FUNCTION"          
	rateDurationEnv = "RATE_DURATION"             
	queryRangeEnv = "QUERY_RANGE"    

	aggregationFunctionDefault = "max"    
	rangeFunctionDefault = "max_over_time" 
	rateFunctionDefault = "irate"          
	rateDurationDefault = "1m"             
	queryRangeDefault = "1d"    
)

var _ framework.PreFilterPlugin = &CarbonAware{}
var _ framework.FilterPlugin = &CarbonAware{}
var _ framework.PreScorePlugin = &CarbonAware{}
var _ framework.ScorePlugin = &CarbonAware{}



type CarbonAware struct {
	handle     framework.Handle
	prometheus *PrometheusHandle
}

type NodeResources struct {
	CPU         int64
	Memory      int64
	APowerLimit int64
	CPowerLimit int64
}

// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	res           framework.Resource
	nodeResources map[string]NodeResources
	podBaseName   string //app.kcas/name
}

// Clone the prefilter state.
func (s *preFilterState) Clone() framework.StateData {
	return s
}

// preScoreState computed at PreScore and used at Score.
type preScoreState struct {
	podInfo       framework.Resource
	nodeResources map[string]NodeResources
	podBaseName   string //app.kcas/name
}

// Utility functions
func getEnvOrDefault(envKey, defaultValue string) string {
	if value := os.Getenv(envKey); value != "" {
		return value
	}
	return defaultValue
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *preScoreState) Clone() framework.StateData {
	return s
}

func (eas *CarbonAware) Name() string {
	return Name
}

// Important to implement this method if you want to use the Score extension point like NormalizeScore.
func (kcas *CarbonAware) ScoreExtensions() framework.ScoreExtensions {
	return kcas
}

// New initializes a new plugin and returns it.
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	args, ok := obj.(*config.CarbonAwareArgs)
	if !ok {
		return nil, fmt.Errorf("[CarbonAware] want args to be of type CarbonAwareArgs, got %T", obj)
	}

	prometheusURL := getEnvOrDefault(prometheusUrlEnv, prometheusUrlDefault)

	prometheusConfig := PrometheusConfig{
		AggregationFunction: getEnvOrDefault(aggregationFunctionEnv, aggregationFunctionDefault),
		RangeFunction: getEnvOrDefault(rangeFunctionEnv, rangeFunctionDefault),
		RateFunction: getEnvOrDefault(rateFunctionEnv, rateFunctionDefault),
		RateDuration: getEnvOrDefault(rateDurationEnv, rateDurationDefault),
		QueryRange: getEnvOrDefault(queryRangeEnv, queryRangeDefault),
	}

	klog.Info("[CarbonAware] Prometheus Config: ", prometheusConfig)


	klog.Infof("[CarbonAware] args received.TimeRangeInMinutes: %d, Address: %s", args.TimeRangeInMinutes, args.Address)

	return &CarbonAware{
		handle:     handle,
		prometheus: NewPrometheus(prometheusURL, prometheusConfig),
	}, nil

}


// This function gets the value of a label from a node
func (kcas *CarbonAware) getNodeLabelValue(nodeName string, labelKey string) (string, error) {
	node, err := kcas.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return "", fmt.Errorf("failed to get node %s: %v", nodeName, err)
	}
	return node.Node().Labels[labelKey], nil
}

func computePodResourceRequest(pod *v1.Pod) *preFilterState {
	// pod hasn't scheduled yet so we don't need to worry about InPlacePodVerticalScalingEnabled
	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})
	podBasName := pod.GetLabels()["app.kcas/name"]
	result := &preFilterState{}
	result.podBaseName = podBasName
	result.res.SetMaxResource(reqs)
	return result
}

// Compute All resources
func (kcas *CarbonAware) computeResources() (map[string]NodeResources, *framework.Status) {

	//nodeLimits := make(map[string]int)
	//result := computePodResourceRequest(pod)
	nodeRes := make(map[string]NodeResources)

	nodeInfos, err := kcas.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		klog.Errorf("Error getting node infos: %v", err)
		// Handle the error appropriately, e.g., return an error or continue with the loop.
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting node infos: %v", err))
	}

	for _, node := range nodeInfos {
		nodeName := node.Node().Name

		constraints0PowerLimit, err := kcas.getNodeLabelValue(nodeName, constraints0PowerLimitStr)
		if err != nil {
			klog.ErrorS(err, "Failed to get power limit from node", "nodeName", nodeName)
			continue
		}

		constraints0PowerLimitC, errC := kcas.getNodeLabelValue(nodeName, constraints0PowerLimitCStr)
		if errC != nil {
			klog.ErrorS(errC, "Failed to get power limit from node", "nodeName", nodeName)
			continue
		}

		powerLimit, err := strconv.ParseInt(constraints0PowerLimit, 10, 64)
		if err != nil {
			klog.ErrorS(err, "Failed to parse power limit", "powerLimit", constraints0PowerLimit, "nodeName", nodeName)
			continue
		}

		powerLimitC, errC := strconv.ParseInt(constraints0PowerLimitC, 10, 64)
		if errC != nil {
			klog.ErrorS(errC, "Failed to parse power limit", "powerLimit", constraints0PowerLimitC)
			continue
		}
		// Get CPU and Memory information from the node
		capacity := node.Node().Status.Capacity
		cpu := capacity.Cpu().MilliValue()
		memory := capacity.Memory().Value() // Convert memory from

		//nodeLimits[nodeName] = powerLimit
		nodeRes[nodeName] = NodeResources{
			CPU:         cpu,
			Memory:      memory,
			APowerLimit: powerLimit,
			CPowerLimit: powerLimitC,
		}
	}

	//result.nodeResources = nodeRes

	return nodeRes, nil
}

// PreFilter invoked at the prefilter extension point.
func (kcas *CarbonAware) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {

	//nodeLimits := make(map[string]int)
	result := computePodResourceRequest(pod)
	nodeRes, err := kcas.computeResources()
	if err != nil {
		return nil, err
	}
	result.nodeResources = nodeRes

	cycleState.Write(preFilterStateKey, result)
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (kcas *CarbonAware) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getPreFilterState(cycleState *framework.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NodeResourcesFit.preFilterState error", c)
	}
	return s, nil
}

// Filter implements framework.FilterPlugin.
func (kcas *CarbonAware) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// Get the prefilter state.
	klog.V(4).Info("Filtering node: ", "node: ", nodeInfo.Node().Name, ", pod: ", pod.Name)
	
	wantPower, err := getPreFilterState(state)
	if err != nil {
		return framework.AsStatus(err)
	}
	fits := kcas.fitsPower(wantPower, nodeInfo)
	if !fits {
		return framework.NewStatus(framework.Unschedulable, ErrReason)
	}
	return nil
}

func (kcas *CarbonAware) fitsPower(wantPower *preFilterState, nodeInfo *framework.NodeInfo) bool {
	nodeName := nodeInfo.Node().Name

	//nodeRes,err = wantPower.nodeResources[nodeName]
	prometheus := kcas.prometheus
	nodeActualConsumption, err := prometheus.getNodePowerMeasure(nodeName)
	klog.V(4).Info("FitsPower: ","node Name: ",nodeName, " , nodeActualConsumption: ", nodeActualConsumption)
	if err != nil {
		klog.Errorf("[CarbonAware] Error getting node power: %v", err)
		return false
	}

	podPower, err := prometheus.getPodTotalPower(wantPower.podBaseName)
	if err != nil {
		klog.Errorf("[CarbonAware] Error getting pod power: %v", err)
			
	}

	klog.Info("podName: ", wantPower.podBaseName)


	nodeRes, ok := wantPower.nodeResources[nodeName]
	if !ok {
		return false
	}
	//podInfo := wantPower.res
	// Get the CPU and Memory information requested by the pod
	//podCPU := podInfo.MilliCPU
	//podMemory := podInfo.Memory

	//klog.V(4).Info("FitsPower: ",  "podCPU: ", podCPU, " , podMemory: ", podMemory)
	//klog.V(4).Info("FitsPower: ","node Name: ",nodeName, " , node CPU: ", nodeRes.CPU, " , node Memory: ", nodeRes.Memory)
	klog.V(4).Info("FitsPower: ","node Name: ",nodeName, " , node APowerLimit: ", nodeRes.APowerLimit, " , node CPowerLimit: ", nodeRes.CPowerLimit)
	// calculate the power needed by the pod
	//podPower := int64(float64(nodeRes.CPowerLimit) * (float64(podCPU)/float64(nodeRes.CPU) + float64(podMemory)/float64(nodeRes.Memory)))
	// check if the node has enough power for the pod
	prI := nodeRes.APowerLimit - nodeActualConsumption

	klog.V(4).Info("FitsPower: ", "node: ", nodeName, " , prI: ", prI, " , podPower: ", podPower)
	return prI >= podPower
}

// PreScore implements framework.PreScorePlugin.
func (kcas *CarbonAware) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	// Get the prefilter state.
	preFilterState, err := getPreFilterState(state)
	if err != nil {
		return framework.AsStatus(err)
	}

	// Compute pod resource request.
	podInfo := computePodResourceRequest(pod).res

	// Filter out nodes that didn't pass the Filter phase.
	filteredNodeResources := make(map[string]NodeResources)
	for _, node := range nodes {
		nodeName := node.Node().Name
		if nodeRes, ok := preFilterState.nodeResources[nodeName]; ok {
			filteredNodeResources[nodeName] = nodeRes
		}
	}

	// Store the preScore state.
	preScoreState := &preScoreState{
		podInfo:       podInfo,
		nodeResources: filteredNodeResources,
		podBaseName:   preFilterState.podBaseName,
	}

	state.Write(preScoreStateKey, preScoreState)
	return nil
}

// getPreScoreState retrieves the preScore state from the cycle state.
func getPreScoreState(cycleState *framework.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to preScoreState error", c)
	}
	return s, nil
}

// Score implements framework.ScorePlugin.
func (kcas *CarbonAware) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	// Implement the scoring logic based on power consumption.
	//klog.V(4).Info("Scoring phase")

	prometheus := kcas.prometheus
	nodeActualConsumption, err := prometheus.getNodePowerMeasure(nodeName)
	if err != nil {
		klog.Errorf("[CarbonAware] Error getting node energy: %v", err)
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting node power: %v", err))

	}

	preScoreState, err := getPreScoreState(state)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	nodeRes, ok := preScoreState.nodeResources[nodeName]
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %s not found in preScoreState", nodeName))
	}

	// podCPU := preScoreState.podInfo.MilliCPU
	// podMemory := preScoreState.podInfo.Memory
	
	

	// podPower := int64(float64(nodeRes.CPowerLimit) * (float64(podCPU)/float64(nodeRes.CPU) + float64(podMemory)/float64(nodeRes.Memory)))
	//podPower = podPower // convert to appropriate unit
	podPower, err := prometheus.getPodTotalPower(preScoreState.podBaseName)
	if err != nil {
		klog.Errorf("[CarbonAware] Error getting pod power: %v", err)
	}
	
	prI := nodeRes.APowerLimit - nodeActualConsumption

	score := prI - podPower

	klog.V(4).Info("Score: ", "pod: ", p.GetName(), " , node: ", nodeName, " , finalScore: ", score)


	return score, nil
}


// // NormalizeScore normalizes the scores of nodes based on their power availability.
// func (kcas *CarbonAware) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
// 	if len(scores) == 0 {
// 		return framework.NewStatus(framework.Error, "no scores to normalize")
// 	}

// 	// Find the min and max scores
// 	minScore, maxScore := scores[0].Score, scores[0].Score
// 	for _, nodeScore := range scores {
// 		if nodeScore.Score < minScore {
// 			minScore = nodeScore.Score
// 		}
// 		if nodeScore.Score > maxScore {
// 			maxScore = nodeScore.Score
// 		}
// 	}

// 	// Normalize the scores
// 	scoreRange := maxScore - minScore
// 	if scoreRange == 0 {
// 		// All scores are the same, set all to the highest possible value
// 		for i := range scores {
// 			scores[i].Score = 100
// 		}
// 	} else {
// 		for i := range scores {
// 			normalizedScore := float64(scores[i].Score-minScore) / float64(scoreRange)
// 			scores[i].Score = int64(normalizedScore)
// 		}
// 	}

// 	return nil
// }

// MinMax : get min and max scores from NodeScoreList
func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	var max int64 = math.MinInt64 // Set to min value
	var min int64 = math.MaxInt64 // Set to max value

	for _, nodeScore := range scores {
		if nodeScore.Score > max {
			max = nodeScore.Score
		}
		if nodeScore.Score < min {
			min = nodeScore.Score
		}
	}
	// return min and max scores
	return min, max
}

// NormalizeScore : normalize scores since lower scores correspond to lower latency
func (kcas *CarbonAware) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("before normalization: ", "scores ", scores)

	// Get Min and Max Scores to normalize between framework.MaxNodeScore and framework.MinNodeScore
	minCost, maxCost := getMinMaxScores(scores)

	// If all nodes were given the minimum score, return
	if minCost == 0 && maxCost == 0 {
		return nil
	}

	var normCost float64
	for i := range scores {
		if maxCost != minCost { // If max != min
			// node_normalized_cost = MAX_SCORE * ( ( nodeScore ) / (maxCost)
			normCost = float64(framework.MaxNodeScore) * float64(scores[i].Score) / float64(maxCost)
			scores[i].Score = int64(normCost)
		} else { // If maxCost = minCost, avoid division by 0
			
			scores[i].Score = framework.MaxNodeScore 
		}
	}
	logger.V(4).Info("after normalization: ", "scores ", scores)
	return nil
}
