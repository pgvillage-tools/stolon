package v1

type InfoCluster struct {
	APIs           InfoAPIs           `json:"apis"`
	Topologies     InfoTopologies     `json:"topologies"`
	Nodes          InfoNodes          `json:"nodes"`
	ConsensusStore InfoConsensusStore `json:"store"`
}

type InfoConsensusStore []InfoConsensusProcess
type InfoConsensusProcess struct {
	Id   string `json:"id"`
	Node string `json:"node"`
}

type InfoAPIs []InfoNode
type InfoAPI struct {
	Uri    string            `json:"uri"`
	Token  string            `json:"token"`
	Status InfoGenericStatus `json:"status"`
}

type InfoNodes map[string]InfoNode
type InfoNode struct {
	Status InfoGenericStatus `json:"status"`
}

type InfoTopologies []InfoTopology
type InfoTopology struct {
	Proxies     InfoProxies       `json:"proxies"`
	ReplicaSets InfoReplicaSets   `json:"replicasets"`
	Status      InfoGenericStatus `json:"status"`
}

type InfoDataStreamType int

const InfoStreamLogicalReplication = iota
const InfoStreamBidirectionalReplication = iota
const InfoStreamKafka = iota

type InfoDataStreams []InfoDataStreams
type InfoDataStream struct {
	Type   InfoDataStreamType `json:"type"`
	Status InfoGenericStatus  `json:"status"`
}

type InfoReplicaSets []InfoReplicaSet
type InfoReplicaSet struct {
	Keepers   InfoKeepers       `json:"keepers"`
	Sentinels InfoSentinels     `json:"sentinels"`
	Status    InfoGenericStatus `json:"status"`
}

type InfoKeepers []InfoKeeper
type InfoKeeper struct {
	UID                 string            `json:"uid"`
	ListenAddress       string            `json:"listen_address"`
	Healthy             bool              `json:"healthy"`
	PgHealthy           bool              `json:"pg_healthy"`
	PgWantedGeneration  int64             `json:"pg_wanted_generation"`
	PgCurrentGeneration int64             `json:"pg_current_generation"`
	Status              InfoGenericStatus `json:"status"`
}

type InfoSentinels []InfoSentinel
type InfoSentinel struct {
	UID    string            `json:"uid"`
	Leader bool              `json:"leader"`
	Status InfoGenericStatus `json:"status"`
}

type InfoProxies []InfoProxy
type InfoProxy struct {
	UID        string            `json:"uid"`
	Generation int64             `json:"generation"`
	Status     InfoGenericStatus `json:"status"`
}

type InfoGenericStatus map[string]any
