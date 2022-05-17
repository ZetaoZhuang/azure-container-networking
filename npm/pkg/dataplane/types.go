package dataplane

import (
	"github.com/Azure/azure-container-networking/npm/pkg/dataplane/ipsets"
	"github.com/Azure/azure-container-networking/npm/pkg/dataplane/policies"
	"github.com/Azure/azure-container-networking/npm/util"
	"github.com/Microsoft/hcsshim/hcn"
)

const minutesToKeepStalePodKey = 10

type GenericDataplane interface {
	BootupDataplane() error
	RunPeriodicTasks()
	GetAllIPSets() []string
	GetIPSet(setName string) *ipsets.IPSet
	CreateIPSets(setMetadatas []*ipsets.IPSetMetadata)
	DeleteIPSet(setMetadata *ipsets.IPSetMetadata, deleteOption util.DeleteOption)
	AddToSets(setMetadatas []*ipsets.IPSetMetadata, podMetadata *PodMetadata) error
	RemoveFromSets(setMetadatas []*ipsets.IPSetMetadata, podMetadata *PodMetadata) error
	AddToLists(listMetadatas []*ipsets.IPSetMetadata, setMetadatas []*ipsets.IPSetMetadata) error
	RemoveFromList(listMetadata *ipsets.IPSetMetadata, setMetadatas []*ipsets.IPSetMetadata) error
	ApplyDataPlane() error
	GetAllPolicies() []string
	AddPolicy(policies *policies.NPMNetworkPolicy) error
	RemovePolicy(PolicyKey string) error
	UpdatePolicy(policies *policies.NPMNetworkPolicy) error
}

// UpdateNPMPod pod controller will populate and send this datastructure to dataplane
// to update the dataplane with the latest pod information
// this helps in calculating if any update needs to have policies applied or removed
type updateNPMPod struct {
	*PodMetadata
	IPSetsToAdd    []string
	IPSetsToRemove []string
}

// PodMetadata is what is passed to dataplane to specify pod ipset
// todo definitely requires further optimization between the intersection
// of types, PodMetadata, NpmPod and corev1.pod
type PodMetadata struct {
	PodKey   string
	PodIP    string
	NodeName string
}

func NewPodMetadata(podKey, podIP, nodeName string) *PodMetadata {
	return &PodMetadata{
		PodKey:   podKey,
		PodIP:    podIP,
		NodeName: nodeName,
	}
}

func newUpdateNPMPod(podMetadata *PodMetadata) *updateNPMPod {
	return &updateNPMPod{
		PodMetadata:    podMetadata,
		IPSetsToAdd:    make([]string, 0),
		IPSetsToRemove: make([]string, 0),
	}
}

func (npmPod *updateNPMPod) updateIPSetsToAdd(setNames []*ipsets.IPSetMetadata) {
	for _, set := range setNames {
		npmPod.IPSetsToAdd = append(npmPod.IPSetsToAdd, set.GetPrefixName())
	}
}

func (npmPod *updateNPMPod) updateIPSetsToRemove(setNames []*ipsets.IPSetMetadata) {
	for _, set := range setNames {
		npmPod.IPSetsToRemove = append(npmPod.IPSetsToRemove, set.GetPrefixName())
	}
}

type npmEndpoint struct {
	name   string
	id     string
	ip     string
	podKey string
	// stalePodKey is used to keep track of the previous pod that had this IP
	stalePodKey *staleKey
	// Map with Key as Network Policy name to to emulate set
	// and value as struct{} for minimal memory consumption
	netPolReference map[string]struct{}
}

func newNPMEndpoint(endpoint *hcn.HostComputeEndpoint) *npmEndpoint {
	return &npmEndpoint{
		name:            endpoint.Name,
		id:              endpoint.Id,
		podKey:          unspecifiedPodKey,
		netPolReference: make(map[string]struct{}),
		ip:              endpoint.IpConfigurations[0].IpAddress,
	}
}

// update creates a new endpoint and marks the old endpoint's pod key as stale
// currentTime should be time.Now().Unix()
func (ep *npmEndpoint) update(endpoint *hcn.HostComputeEndpoint, currentTime int64) *npmEndpoint {
	newEP := newNPMEndpoint(endpoint)
	newEP.stalePodKey = &staleKey{
		key:       ep.podKey,
		timestamp: currentTime,
	}
	return newEP
}

// shouldDelete if enough time has passed since the last update.
// Should pass in time.Now().Unix() as the current time.
func (ep *npmEndpoint) shouldDelete(currentTime int64) bool {
	return ep.stalePodKey == nil || ep.stalePodKey.minutesElapsed(currentTime) > minutesToKeepStalePodKey
}

type staleKey struct {
	key string
	// timestamp represents the Unix time this struct was created
	timestamp int64
}

func (spk *staleKey) minutesElapsed(currentTime int64) int {
	return int(currentTime-spk.timestamp) / 60
}
