package integration

import "github.com/sorintlab/stolon/internal/cluster"

var (
	newCluster      = cluster.New
	pitrCluster     = cluster.PITR
	existingCluster = cluster.ExistingCluster
	replica         = cluster.Replica
)
