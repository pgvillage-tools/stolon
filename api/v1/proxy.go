package cluster

import "time"

// ProxySpec defines the config of a Proxy instance
type ProxySpec struct {
	MasterDBUID    string   `json:"masterDbUid,omitempty"`
	EnabledProxies []string `json:"enabledProxies,omitempty"`
}

// ProxyStatus defines the status of a Proxy instance
type ProxyStatus struct {
}

// Proxy combines the Spec, Status and other config of a Proxy into one resource
type Proxy struct {
	UID        string    `json:"uid,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	ChangeTime time.Time `json:"changeTime,omitempty"`

	Spec ProxySpec `json:"spec,omitempty"`

	Status ProxyStatus `json:"status,omitempty"`
}
