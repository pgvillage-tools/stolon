// Copyright 2025 Nibble-IT
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresql

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("moveDirRecursive", func() {
	var (
		randomString = func() string { return fmt.Sprintf("%x", rand.Int63()) }
		randomBytes  = func() []byte { return []byte(fmt.Sprintf("%x", rand.Int63())) }
	)
	When("moving a directory", func() {
		It("should successfully move", func() {
			const (
				walDir    = "wal"
				wal1      = walDir + "1"
				wal2      = walDir + "2"
				walSubDir = walDir + "/subdir"
			)
			for _, test := range []struct {
				src      string
				dst      string
				cleanSrc bool
			}{
				{src: wal1, dst: wal2, cleanSrc: true},
				{src: walDir, dst: walSubDir},
				{src: walSubDir, dst: walDir, cleanSrc: true},
			} {
				var expected = map[string][]byte{}
				var unExpected []string
				fmt.Fprintf(GinkgoWriter, "DEBUG - Test: %v\n", test)
				tempDir, err := os.MkdirTemp("", "moveDirRecursive")
				Ω(err).NotTo(HaveOccurred())
				defer os.RemoveAll(tempDir) // Clean up after test
				srcDir := filepath.Join(tempDir, test.src)
				err = os.MkdirAll(srcDir, uRWX)
				Ω(err).NotTo(HaveOccurred())
				dstDir := filepath.Join(tempDir, test.dst)
				for _, subDir := range []string{
					"",
					randomString(),
					filepath.Join(randomString(), randomString()),
				} {
					if subDir != "" {
						unExpected = append(unExpected, filepath.Join(srcDir, subDir))
					}
					err = os.MkdirAll(filepath.Join(srcDir, subDir), uRWX)
					Ω(err).NotTo(HaveOccurred())
					for i := 0; i < 10; i++ {
						filePath := filepath.Join(subDir, randomString())
						data := randomBytes()
						expected[filepath.Join(dstDir, filePath)] = data
						err = os.WriteFile(filepath.Join(srcDir, filePath), data, uRW)
						Ω(err).NotTo(HaveOccurred())
					}
				}
				err = moveDir(context.Background(), srcDir, dstDir)
				Ω(err).NotTo(HaveOccurred())
				for path, content := range expected {
					data, err := os.ReadFile(path)
					Ω(err).NotTo(HaveOccurred())
					Ω(data).To(Equal(content))
				}
				for _, path := range unExpected {
					_, err := os.Stat(path)
					Ω(err).To(HaveOccurred())
					Ω(err).To(MatchError(os.ErrNotExist))
				}
				if test.cleanSrc {
					_, err := os.Stat(srcDir)
					Ω(err).To(HaveOccurred())
					Ω(err).To(MatchError(os.ErrNotExist))
				}
			}
		})
		It("should fail soon", func() {
		})
	})
})
