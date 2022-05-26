package dataplane

// npmEndpoint holds info relevant for endpoints in windows
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

// newNPMEndpoint initializes npmEndpoint and copies relevant information from hcn.HostComputeEndpoint.
// This function must be defined in a file with a windows build tag for proper vendoring since it uses the hcn pkg
func newNPMEndpoint(endpoint *hcn.HostComputeEndpoint) *npmEndpoint {
	return &npmEndpoint{
		name:            endpoint.Name,
		id:              endpoint.Id,
		podKey:          unspecifiedPodKey,
		netPolReference: make(map[string]struct{}),
		ip:              endpoint.IpConfigurations[0].IpAddress,
	}
}

// update creates a new endpoint and marks the old endpoint's pod key as stale.
// currentTime should be time.Now().Unix()
// This function must be defined in a file with a windows build tag for proper vendoring since it uses the hcn pkg
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
