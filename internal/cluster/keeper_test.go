package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 39-51 57-63

func TestKeeperSortedKeys(t *testing.T) {
	const (
		numKeepers = 9
	)
	keepersInfo := KeepersInfo{}
	keepers := Keepers{}
	for i := numKeepers; i > 0; i-- {
		id := fmt.Sprintf("k%d", i)
		uid := fmt.Sprintf("id%d", i)
		newUID := fmt.Sprintf("newid%d", i)
		bootUUID := fmt.Sprintf("boot%d", i)
		info := &KeeperInfo{UID: uid, BootUUID: bootUUID}
		keepersInfo[id] = info
		info = info.DeepCopy()
		info.UID = newUID
		keeper := NewKeeperFromKeeperInfo(info)
		assert.Equal(t, newUID, keeper.UID)
		assert.Equal(t, bootUUID, keeper.Status.BootUUID)
		keepers[id] = keeper
	}
	var expectedSortedKeys []string
	for i := 1; i <= numKeepers; i++ {
		expectedSortedKeys = append(expectedSortedKeys, fmt.Sprintf("k%d", i))
	}
	newKeepersInfo := keepersInfo.DeepCopy()
	newKeepersInfo["k1"].UID = "otherid1"
	newKeepersInfo["k9"].BootUUID = "otherboot9"
	assert.Equal(t, expectedSortedKeys, keepers.SortedKeys())
	assert.Equal(t, "id1", keepersInfo["k1"].UID)
	assert.Equal(t, "boot9", keepersInfo["k9"].BootUUID)
	assert.Equal(t, "newid1", keepers["k1"].UID)
	assert.Equal(t, "boot9", keepers["k9"].Status.BootUUID)
}
