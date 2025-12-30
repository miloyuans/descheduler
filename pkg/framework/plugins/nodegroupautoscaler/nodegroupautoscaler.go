/*
Copyright 2025 Your Name or Organization.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodegroupautoscaler

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eks"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/api/v1alpha2"
	"sigs.k8s.io/descheduler/pkg/descheduler/node"
	"sigs.k8s.io/descheduler/pkg/framework"
	"sigs.k8s.io/descheduler/pkg/framework/pluginregistry"
	"sigs.k8s.io/descheduler/pkg/framework/types"
)

// PluginName is the name of the plugin.
const PluginName = "NodeGroupAutoscaler"

// NodeGroupAutoscalerArgs holds arguments for the NodeGroupAutoscaler plugin.
type NodeGroupAutoscalerArgs struct {
	v1alpha2.Args `json:",inline"`

	HighThreshold  int             `json:"highThreshold,omitempty"`  // 默认80
	LowThreshold   int             `json:"lowThreshold,omitempty"`   // 默认30
	MaxThreshold   int             `json:"maxThreshold,omitempty"`   // 默认70
	ClusterName    string          `json:"clusterName,omitempty"`    // 必需
	NodeGroupNames []string        `json:"nodeGroupNames,omitempty"` // 可选
	CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"` // 默认5m
}

// Validate validates the args.
func (args *NodeGroupAutoscalerArgs) Validate() *types.Status {
	if args.ClusterName == "" {
		return &types.Status{Err: "clusterName is required"}
	}
	if args.HighThreshold <= args.LowThreshold || args.HighThreshold > 100 || args.LowThreshold < 0 {
		return &types.Status{Err: "invalid thresholds"}
	}
	if args.MaxThreshold == 0 {
		args.MaxThreshold = 70
	}
	if args.CooldownPeriod.Duration == 0 {
		args.CooldownPeriod.Duration = 5 * time.Minute
	}
	return nil
}

// New builds a NodeGroupAutoscaler plugin from the given args.
func New(args runtime.RawExtension, h framework.Handle) (framework.Plugin, error) {
	pluginArgs := &NodeGroupAutoscalerArgs{}
	if err := framework.DecodeInto(args, pluginArgs); err != nil {
		return nil, err
	}
	if status := pluginArgs.Validate(); status != nil && status.Err != "" {
		return nil, fmt.Errorf(status.Err)
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	return &NodeGroupAutoscaler{
		handle:             h,
		args:               *pluginArgs,
		awsSess:            sess,
		eksSvc:             eks.New(sess),
		lastScaleDownTimes: make(map[string]time.Time),
	}, nil
}

type NodeGroupAutoscaler struct {
	handle             framework.Handle
	args               NodeGroupAutoscalerArgs
	awsSess            *session.Session
	eksSvc             *eks.EKS
	lastScaleDownTimes map[string]time.Time
}

// Name returns the plugin name.
func (p *NodeGroupAutoscaler) Name() string {
	return PluginName
}

// Balance is the main entry point for balance plugins.
func (p *NodeGroupAutoscaler) Balance(ctx context.Context, nodes []*v1.Node) *types.Status {
	defer utilruntime.HandleCrash()

	nodegroups, err := p.getEKSNodeGroups()
	if err != nil {
		klog.ErrorS(err, "failed to get nodegroups")
		return &types.Status{Err: err.Error()}
	}

	for _, ng := range nodegroups {
		ngName := *ng.NodegroupName
		if len(p.args.NodeGroupNames) > 0 && !contains(p.args.NodeGroupNames, ngName) {
			continue
		}

		nodesInGroup, err := p.getNodesInGroup(ngName)
		if err != nil || len(nodesInGroup) <= int(*ng.ScalingConfig.MinSize) {
			continue
		}

		avgUtil, nodeUtils := p.calculateAvgUtilizationAndPerNode(nodesInGroup)
		lowUtilNode := p.selectUnderutilizedNode(nodeUtils, avgUtil)

		if lowUtilNode != nil && p.isImbalanced(nodesInGroup, nodeUtils) {
			// Scale up if high
			if avgUtil > float64(p.args.HighThreshold)/100 {
				if err := p.scaleNodeGroup(ng, 1); err != nil {
					klog.ErrorS(err, "failed to scale up", "nodegroup", ngName)
				} else {
					klog.V(1).Infof("Scaled up nodegroup %s by 1", ngName)
				}
				continue
			}

			// Scale down if low, simulated ok, cooldown passed
			if avgUtil < float64(p.args.LowThreshold)/100 && *ng.ScalingConfig.DesiredSize > *ng.ScalingConfig.MinSize {
				lastTime := p.lastScaleDownTimes[ngName]
				if time.Since(lastTime) < p.args.CooldownPeriod.Duration {
					klog.V(2).Infof("Cooldown active for nodegroup %s", ngName)
					continue
				}

				if p.simulateBalanceAfterRemoval(nodesInGroup, lowUtilNode) {
					if err := p.cordonNode(lowUtilNode); err != nil {
						klog.ErrorS(err, "failed to cordon node", "node", lowUtilNode.Name)
						continue
					}
					if err := p.evictPodsFromNode(lowUtilNode); err != nil {
						klog.ErrorS(err, "failed to evict pods", "node", lowUtilNode.Name)
						continue
					}
					if err := p.scaleNodeGroup(ng, -1); err != nil {
						klog.ErrorS(err, "failed to scale down", "nodegroup", ngName)
					} else {
						p.lastScaleDownTimes[ngName] = time.Now()
						klog.V(1).Infof("Scaled down nodegroup %s by 1 after simulation", ngName)
					}
				} else {
					klog.V(2).Infof("Simulation failed for %s", ngName)
				}
			}
		}
	}
	return nil
}

// getEKSNodeGroups 获取当前 EKS 集群的所有 nodegroups
func (p *NodeGroupAutoscaler) getEKSNodeGroups() ([]*eks.Nodegroup, error) {
	var nodegroups []*eks.Nodegroup
	input := &eks.ListNodegroupsInput{ClusterName: aws.String(p.args.ClusterName)}
	output, err := p.eksSvc.ListNodegroups(input)
	if err != nil {
		return nil, err
	}
	for _, ngName := range output.Nodegroups {
		describeInput := &eks.DescribeNodegroupInput{ClusterName: aws.String(p.args.ClusterName), NodegroupName: ngName}
		describeOutput, err := p.eksSvc.DescribeNodegroup(describeInput)
		if err != nil {
			return nil, err
		}
		nodegroups = append(nodegroups, describeOutput.Nodegroup)
	}
	return nodegroups, nil
}

// getNodesInGroup 根据标签获取 nodegroup 中的节点
func (p *NodeGroupAutoscaler) getNodesInGroup(nodegroupName string) ([]*v1.Node, error) {
	selector := labels.SelectorFromSet(labels.Set{"eks.amazonaws.com/nodegroup": nodegroupName})
	list, err := p.handle.SharedInformerFactory().Core().V1().Nodes().Lister().List(selector)
	if err != nil {
		return nil, err
	}
	return list, nil
}

// calculateAvgUtilizationAndPerNode 计算平均和每个节点的利用率 (使用 node.GetNodeAllPodsUtilization, CPU 示例)
func (p *NodeGroupAutoscaler) calculateAvgUtilizationAndPerNode(nodes []*v1.Node) (float64, map[string]float64) {
	var totalUtil float64
	nodeUtils := make(map[string]float64)
	for _, node := range nodes {
		pods, _ := p.handle.GetPodsAssignedToNode(node.Name)
		utilList, _ := node.GetNodeAllPodsUtilization(pods, []v1.ResourceName{v1.ResourceCPU}, nil)
		util := float64(utilList[v1.ResourceCPU].MilliValue()) / float64(node.Status.Allocatable.Cpu().MilliValue())
		nodeUtils[node.Name] = util
		totalUtil += util
	}
	avg := 0.0
	if len(nodes) > 0 {
		avg = totalUtil / float64(len(nodes))
	}
	return avg, nodeUtils
}

// isImbalanced 检查是否不均衡：存在低于 avg 的节点，且其他 <= max
func (p *NodeGroupAutoscaler) isImbalanced(nodes []*v1.Node, nodeUtils map[string]float64) bool {
	hasLow := false
	avgUtil := p.calculateAvgUtilizationAndPerNode(nodes)[0]
	for _, util := range nodeUtils {
		if util > float64(p.args.MaxThreshold)/100 {
			return false // 有超 max，不平衡但不能缩
		}
		if util < avgUtil {
			hasLow = true
		}
	}
	return hasLow
}

// selectUnderutilizedNode 选择最低利用节点
func (p *NodeGroupAutoscaler) selectUnderutilizedNode(nodeUtils map[string]float64, avgUtil float64) *v1.Node {
	var lowNode *v1.Node
	var minUtil float64 = 1.0
	for nodeName, util := range nodeUtils {
		if util < avgUtil && util < minUtil {
			minUtil = util
			node, _ := p.handle.SharedInformerFactory().Core().V1().Nodes().Lister().Get(nodeName)
			lowNode = node
		}
	}
	return lowNode
}

// simulateBalanceAfterRemoval 模拟删除后均衡，使用 node.GetNodeAllPodsUtilization 计算总请求
func (p *NodeGroupAutoscaler) simulateBalanceAfterRemoval(nodes []*v1.Node, removeNode *v1.Node) bool {
	totalReqCPU := int64(0)
	for _, node := range nodes {
		pods, _ := p.handle.GetPodsAssignedToNode(node.Name)
		reqList, _ := node.GetNodeAllPodsUtilization(pods, []v1.ResourceName{v1.ResourceCPU}, nil)
		totalReqCPU += reqList[v1.ResourceCPU].MilliValue()
	}

	remainingNodes := make([]*v1.Node, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.Name != removeNode.Name {
			remainingNodes = append(remainingNodes, n)
		}
	}
	if len(remainingNodes) == 0 {
		return false
	}

	avgReqPerNode := totalReqCPU / int64(len(remainingNodes))

	for _, node := range remainingNodes {
		allocCPU := node.Status.Allocatable.Cpu().MilliValue()
		projectedUtil := float64(avgReqPerNode) / float64(allocCPU)
		if projectedUtil > float64(p.args.MaxThreshold)/100 {
			return false // 超 max，不行
		}
	}
	return true
}

// cordonNode 标记节点 unschedulable
func (p *NodeGroupAutoscaler) cordonNode(node *v1.Node) error {
	copyNode := node.DeepCopy()
	copyNode.Spec.Unschedulable = true
	_, err := p.handle.ClientSet().CoreV1().Nodes().Update(context.TODO(), copyNode, metav1.UpdateOptions{})
	return err
}

// evictPodsFromNode 优雅驱逐节点上所有 pods
func (p *NodeGroupAutoscaler) evictPodsFromNode(node *v1.Node) error {
	pods, err := p.handle.GetPodsAssignedToNode(node.Name)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		status := p.handle.Evictor().Evict(context.TODO(), pod, types.EvictOptions{})
		if status != nil && status.Err != "" {
			return fmt.Errorf(status.Err)
		}
	}
	// 等待驱逐完成 (轮询节点 pods 为0)
	err = wait.PollImmediate(10*time.Second, 5*time.Minute, func() (bool, error) {
		pods, _ := p.handle.GetPodsAssignedToNode(node.Name)
		return len(pods) == 0, nil
	})
	return err
}

// scaleNodeGroup 更新 nodegroup 大小
func (p *NodeGroupAutoscaler) scaleNodeGroup(ng *eks.Nodegroup, delta int64) error {
	newDesired := *ng.ScalingConfig.DesiredSize + delta
	if newDesired < *ng.ScalingConfig.MinSize || newDesired > *ng.ScalingConfig.MaxSize {
		return fmt.Errorf("desired size out of bounds")
	}
	input := &eks.UpdateNodegroupConfigInput{
		ClusterName:   aws.String(p.args.ClusterName),
		NodegroupName: ng.NodegroupName,
		ScalingConfig: &eks.NodegroupScalingConfig{
			DesiredSize: aws.Int64(newDesired),
		},
	}
	_, err := p.eksSvc.UpdateNodegroupConfig(input)
	return err
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func init() {
	pluginregistry.Register(PluginName, New, types.BalancePlugin, &NodeGroupAutoscalerArgs{}, nil, nil, pluginregistry.PluginRegistry)
}