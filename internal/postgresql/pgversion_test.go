package postgresql

import (
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pgversion", func() {
	When("Parsing version from a binary", func() {
		It("should successfully parse", func() {
			for _, test := range []struct {
				in       string
				expected string
			}{
				{in: "v10", expected: "10.0.0"},
				{in: "10", expected: "10.0.0"},
				{in: "9.5.7", expected: "9.5.7"},
				{in: "9.5.7-rc1", expected: "9.5.7-rc1"},
				{in: "11-beta1", expected: "11.0.0-beta1"},
			} {
				fmt.Fprintf(GinkgoWriter, "DEBUG - Test: %v\n", test)
				version, err := parseVersion(test.in)
				Ω(err).NotTo(HaveOccurred())
				Ω(version.String()).To(Equal(test.expected))
			}
		})
		It("should fail on", func() {
			for _, test := range []struct {
				in string
			}{
				{in: "v"},
				{in: "1.2.3.4"},
			} {
				fmt.Fprintf(GinkgoWriter, "DEBUG - Test: %v\n", test)
				version, err := parseVersion(test.in)
				Ω(err).To(HaveOccurred())
				Ω(version).To(BeNil())
			}
		})
	})
	When("Comparing", func() {
		It("should properly check greater than or equal", func() {
			for _, test := range []struct {
				in string
				ge string
			}{
				{in: "10", ge: "10"},
				{in: "10", ge: "9.6"},
				{in: "9.6", ge: "9.5.7"},
				{in: "9.5.7", ge: "9.5.7-rc1"},
				{in: "9.5.7-rc1", ge: "9.5.7-beta1"},
				{in: "9.6-beta1", ge: "9.5.7-rc1"},
			} {
				fmt.Fprintf(GinkgoWriter, "DEBUG - Test: %v\n", test)
				inVersion, err := parseVersion(test.in)
				Ω(err).NotTo(HaveOccurred())
				geVersion, err := parseVersion(test.ge)
				Ω(err).NotTo(HaveOccurred())
				Ω(inVersion.GreaterThanEqual(geVersion)).To(BeTrue())
			}
		})
	})
	When("Getting version from PGDATA", func() {
		It("should successfully work for a proper DATADIR", func() {
			tempDir, err := os.MkdirTemp("", "pgdvs")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			dataDir := filepath.Join(tempDir, "proper_dir")
			err = os.Mkdir(dataDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
			err = os.WriteFile(pgVersionFile, []byte("18"), 0o600)
			version, err := pgDataVersion(dataDir)
			Ω(err).NotTo(HaveOccurred())
			Ω(version.Equal(V18)).To(BeTrue())
		})
		It("should fail when DATADIR does not exist", func() {
			tempDir, err := os.MkdirTemp("", "pgdvd")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			dataDir := filepath.Join(tempDir, "proper_dir")
			version, err := pgDataVersion(dataDir)
			Ω(err).To(HaveOccurred())
			Ω(version).To(BeNil())
		})
		It("should fail when PGVERSION does not exist", func() {
			tempDir, err := os.MkdirTemp("", "pgdvv")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			dataDir := filepath.Join(tempDir, "proper_dir")
			err = os.Mkdir(dataDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			version, err := pgDataVersion(dataDir)
			Ω(err).To(HaveOccurred())
			Ω(version).To(BeNil())
		})
		It("should fail when PGVERSION contains nonsense", func() {
			tempDir, err := os.MkdirTemp("", "pgdvs")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			dataDir := filepath.Join(tempDir, "proper_dir")
			err = os.Mkdir(dataDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
			err = os.WriteFile(pgVersionFile, []byte("aabbccd"), 0o600)
			version, err := pgDataVersion(dataDir)
			Ω(err).To(HaveOccurred())
			Ω(version).To(BeNil())
		})
	})
	When("Getting version from a binary", func() {
		It("should successfully work for a proper postgres binary", func() {
			tempDir, err := os.MkdirTemp("", "pgbvs")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			binDir := filepath.Join(tempDir, "bindir")
			err = os.Mkdir(binDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			pgBinary := filepath.Join(binDir, "postgres")
			err = os.WriteFile(pgBinary, []byte(
				`#!/bin/bash
echo "postgres (PostgreSQL) 18.0 (Debian 18.0-1.pgdg13+3)"`,
			), 0o700)
			version, err := binaryVersion(binDir)
			Ω(err).NotTo(HaveOccurred())
			Ω(version.Equal(V18)).To(BeTrue())
		})
		It("should fail when binary does not exist", func() {
			tempDir, err := os.MkdirTemp("", "pgbvb")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			binDir := filepath.Join(tempDir, "bindir")
			err = os.Mkdir(binDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			version, err := binaryVersion(binDir)
			Ω(err).To(HaveOccurred())
			Ω(version).To(BeNil())
		})
		It("should fail when binary returns nonsense", func() {
			tempDir, err := os.MkdirTemp("", "pgbvb")
			Ω(err).NotTo(HaveOccurred())
			defer os.RemoveAll(tempDir) // Clean up after test
			binDir := filepath.Join(tempDir, "bindir")
			err = os.Mkdir(binDir, 0o700) // revive:disable-line:add-constant
			Ω(err).NotTo(HaveOccurred())
			pgBinary := filepath.Join(binDir, "postgres")
			err = os.WriteFile(pgBinary, []byte(
				`#!/bin/bash
echo "this is no good"`,
			), 0o700)
			version, err := binaryVersion(binDir)
			Ω(err).To(HaveOccurred())
			Ω(version).To(BeNil())
		})
	})
})
