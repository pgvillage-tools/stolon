// Copyright 2025 Nibble-IT
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

// Package postgresql has all stolon specific postgresql code
package postgresql

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/Masterminds/semver/v3"
)

var (
	// V95 represents PostgreSQL 9.5
	V95 = semver.MustParse("9.5")
	// V96 represents PostgreSQL 9.6
	V96 = semver.MustParse("9.6")
	// V10 represents PostgreSQL 10
	V10 = semver.MustParse("10")
	// V12 represents PostgreSQL 12
	V12 = semver.MustParse("12")
	// V13 represents PostgreSQL 13
	V13 = semver.MustParse("13")
	// V18 represents PostgreSQL 18
	V18 = semver.MustParse("18")
)

func parseBinaryVersion(v string) (*semver.Version, error) {
	// extract version (removing beta*, rc* etc...)

	regex := regexp.MustCompile(`.* \(PostgreSQL\) ([0-9\.]+).*`)
	m := regex.FindStringSubmatch(v)
	if len(m) != 2 {
		return nil, fmt.Errorf("failed to parse postgres binary version: %q", v)
	}
	return semver.NewVersion(m[1])
}

func parseVersion(v string) (*semver.Version, error) {
	return semver.NewVersion(v)
}

func pgDataVersion(dataDir string) (*semver.Version, error) {
	fh, err := os.Open(filepath.Join(dataDir, "PG_VERSION"))
	if err != nil {
		return nil, fmt.Errorf("failed to read PG_VERSION: %v", err)
	}
	defer handledFileClose(fh)

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()

	version := scanner.Text()
	return parseVersion(version)
}

func binaryVersion(binPath string) (*semver.Version, error) {
	name := filepath.Join(binPath, "postgres")
	cmd := exec.Command(name, "-V")
	log.Debugw("execing cmd", logCmd, cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return parseBinaryVersion(string(out))
}
