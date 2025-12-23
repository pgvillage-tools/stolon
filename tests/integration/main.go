package integration

import "github.com/sorintlab/stolon/internal/cluster"

var (
	newCluster      = cluster.New
	pitrCluster     = cluster.PITR
	existingCluster = cluster.Existing
	replica         = cluster.Replica
)
