package flagutil

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFlagutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Flagutil Suite")
}
