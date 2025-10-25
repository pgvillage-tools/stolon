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
	V95 = semver.MustParse("9.5")
	V96 = semver.MustParse("9.6")
	V10 = semver.MustParse("10")
	V11 = semver.MustParse("11")
	V12 = semver.MustParse("12")
	V13 = semver.MustParse("13")
	V14 = semver.MustParse("14")
	V15 = semver.MustParse("15")
	V16 = semver.MustParse("16")
	V17 = semver.MustParse("17")
)

func parseBinaryVersion(v string) (*semver.Version, error) {
	// extract version (removing beta*, rc* etc...)

	regex, err := regexp.Compile(`.* \(PostgreSQL\) ([0-9\.]+).*`)
	if err != nil {
		return nil, err
	}
	m := regex.FindStringSubmatch(v)
	if len(m) != 2 {
		return nil, fmt.Errorf("failed to parse postgres binary version: %q", v)
	}
	return semver.NewVersion(m[1])
}

func ParseVersion(v string) (*semver.Version, error) {
	return semver.NewVersion(v)
}

func (p *Manager) PGDataVersion() (*semver.Version, error) {
	fh, err := os.Open(filepath.Join(p.dataDir, "PG_VERSION"))
	if err != nil {
		return nil, fmt.Errorf("failed to read PG_VERSION: %v", err)
	}
	defer handledFileClose(fh)

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	scanner.Scan()

	version := scanner.Text()
	return ParseVersion(version)
}

func (p *Manager) BinaryVersion() (*semver.Version, error) {
	name := filepath.Join(p.pgBinPath, "postgres")
	cmd := exec.Command(name, "-V")
	log.Debugw("execing cmd", "cmd", cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error: %v, output: %s", err, string(out))
	}

	return parseBinaryVersion(string(out))
}
