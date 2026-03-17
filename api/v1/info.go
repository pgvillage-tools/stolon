package v1

type InfoCluster struct {
	Proxies   InfoProxies       `json:"proxies"`
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
