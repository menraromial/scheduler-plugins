package carbonaware

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"time"
	"go.etcd.io/etcd/clientv3"
)

const (
	Name          = "CarbonAware"
	prometheusURL = "http://10.128.0.3:9090/api/v1/query?query=kepler_node_package_joules_total"
	preFilterStateKey = "PreFilter" + Name
	preScoreStateKey  = "PreScore" + Name
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
	prometheus    *PrometheusHandle
	etcdClient *clientv3.Client
}


type NodeResources struct {
    CPU    int64
    Memory int64
	APowerLimit int64
	CPowerLimit int64
}

// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	res framework.Resource
	nodeResources map[string]NodeResources
}

// Clone the prefilter state.
func (s *preFilterState) Clone() framework.StateData {
	return s
}

// preScoreState computed at PreScore and used at Score.
type preScoreState struct {
	// podRequests have the same order as the resources defined in NodeResourcesBalancedAllocationArgs.Resources,
	// same for other place we store a list like that.
	podRequests []int64
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

	// Configuration de l'initialisation du client etcd
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"http://etcd.default.svc.cluster.local:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create etcd client: %v", err)
    }

	klog.Infof("[CarbonAware] args received.TimeRangeInMinutes: %d, Address: %s", args.TimeRangeInMinutes, args.Address)

	return &CarbonAware{
		handle:        handle,
		prometheus:    NewPrometheus(args.Address, time.Minute*time.Duration(args.TimeRangeInMinutes)),
		etcdClient: cli,
	}, nil

}




func computePodResourceRequest(pod *v1.Pod) *preFilterState {
	// pod hasn't scheduled yet so we don't need to worry about InPlacePodVerticalScalingEnabled
	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})
	result := &preFilterState{}
	result.res.SetMaxResource(reqs)
	return result
}

// PreFilter invoked at the prefilter extension point.
func (kcas *CarbonAware) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	
	//nodeLimits := make(map[string]int)
	result := computePodResourceRequest(pod)
	nodeRes := make(map[string]NodeResources)

	nodeInfos, err := kcas.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		klog.Errorf("Error getting node infos: %v", err)
		// Handle the error appropriately, e.g., return an error or continue with the loop.
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting node infos: %v", err))
	}
	
	for _, node := range nodeInfos {
        nodeName := node.Node().Name
        key := fmt.Sprintf("/pLimits/actual/0/%s", nodeName)
        keyC := fmt.Sprintf("/pLimits/0/%s", nodeName)

        resp, err := kcas.etcdClient.Get(ctx, key)
		respC, errC := kcas.etcdClient.Get(ctx, keyC)
		if err != nil || errC != nil {
            klog.ErrorS(err, "Failed to get power limit from etcd", "nodeName", nodeName)
			continue
        }

        if len(resp.Kvs) == 0 || len(respC.Kvs) == 0 {
            klog.ErrorS(fmt.Errorf("no value found for key"), "Failed to get power limit from etcd", "nodeName", nodeName, "key", key)
            continue
        }

        var powerLimit int64
		var powerLimitC int64
        if err := json.Unmarshal(resp.Kvs[0].Value, &powerLimit); err != nil {
            klog.ErrorS(err, "Failed to unmarshal etcd response", "nodeName", nodeName)
            continue
        }
		if errC := json.Unmarshal(respC.Kvs[0].Value, &powerLimitC); errC != nil {
			klog.ErrorS(errC, "Failed to unmarshal etcd response", "nodeName", nodeName)
			continue
		}
		// Get CPU and Memory information from the node
        capacity := node.Node().Status.Capacity
        cpu := capacity.Cpu().MilliValue()
        memory := capacity.Memory().Value() // Convert memory from

        //nodeLimits[nodeName] = powerLimit
		nodeRes[nodeName] = NodeResources{
			CPU: cpu,
			Memory: memory,
			APowerLimit: powerLimit,
			CPowerLimit: powerLimitC,
		}
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





func (eas *CarbonAware) ScoreExtensions() framework.ScoreExtensions {
	return nil
}



