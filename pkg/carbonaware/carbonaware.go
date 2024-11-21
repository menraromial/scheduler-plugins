package carbonaware

import (
	"context"
	"fmt"
	"strconv"
	//"time"

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

	prometheusURL = "http://prometheus-server.default.svc.cluster.local"
)

var _ framework.PreFilterPlugin = &CarbonAware{}
var _ framework.FilterPlugin = &CarbonAware{}
var _ framework.PreScorePlugin = &CarbonAware{}
var _ framework.ScorePlugin = &CarbonAware{}

//var _ framework.NodeScoreExtension = &CarbonAware{}

/* EnergyMetrics holds the energy metrics for nodes
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
*/

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
}

// Clone the prefilter state.
func (s *preFilterState) Clone() framework.StateData {
	return s
}

// preScoreState computed at PreScore and used at Score.
type preScoreState struct {
	podInfo       framework.Resource
	nodeResources map[string]NodeResources
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *preScoreState) Clone() framework.StateData {
	return s
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
		handle:     handle,
		prometheus: NewPrometheus(prometheusURL),
	}, nil

}

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
	result := &preFilterState{}
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
			klog.ErrorS(err, "Failed to parse power limit", "powerLimit", constraints0PowerLimit)
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
	nodeActualConsumption, err := prometheus.GetNodePowerMeasure(nodeName)
	if err != nil {
		klog.Errorf("[CarbonAware] Error getting node power: %v", err)
		return false
	}

	nodeRes, ok := wantPower.nodeResources[nodeName]
	if !ok {
		return false
	}
	podInfo := wantPower.res
	// Get the CPU and Memory information requested by the pod
	podCPU := podInfo.MilliCPU
	podMemory := podInfo.Memory
	// calculate the power needed by the pod
	podPower := nodeRes.CPowerLimit * (podCPU/nodeRes.CPU + podMemory/nodeRes.Memory)
	// check if the node has enough power for the pod
	prI := nodeRes.CPowerLimit - nodeActualConsumption
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
	prometheus := kcas.prometheus
	nodeActualConsumption, err := prometheus.GetNodePowerMeasure(nodeName)
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

	podCPU := preScoreState.podInfo.MilliCPU
	podMemory := preScoreState.podInfo.Memory

	podPower := nodeRes.CPowerLimit * (podCPU/nodeRes.CPU + podMemory/nodeRes.Memory)
	//podPower = podPower // convert to appropriate unit
	pr_i := nodeRes.CPowerLimit - nodeActualConsumption

	score := pr_i - podPower

	return score, nil
}

func (eas *CarbonAware) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// NormalizeScore normalizes the scores of nodes based on their power availability.
func (kcas *CarbonAware) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	if len(scores) == 0 {
		return framework.NewStatus(framework.Error, "no scores to normalize")
	}

	// Find the min and max scores
	minScore, maxScore := scores[0].Score, scores[0].Score
	for _, nodeScore := range scores {
		if nodeScore.Score < minScore {
			minScore = nodeScore.Score
		}
		if nodeScore.Score > maxScore {
			maxScore = nodeScore.Score
		}
	}

	// Normalize the scores
	scoreRange := maxScore - minScore
	if scoreRange == 0 {
		// All scores are the same, set all to the highest possible value
		for i := range scores {
			scores[i].Score = framework.MaxNodeScore
		}
	} else {
		for i := range scores {
			normalizedScore := float64(scores[i].Score-minScore) / float64(scoreRange)
			scores[i].Score = int64(normalizedScore)
		}
	}

	return nil
}
