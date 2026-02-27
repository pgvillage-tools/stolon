package integration

import cluster "github.com/sorintlab/stolon/api/v1"

var (
	newCluster      = cluster.New
	pitrCluster     = cluster.PITR
	existingCluster = cluster.ExistingCluster
	replica         = cluster.Replica
)
