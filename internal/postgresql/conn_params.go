// Copyright 2015 Sorint.lab
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
	"errors"
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

// This is based on github.com/lib/pq

// ConnParams defines key/value pairs for connecting to PostgreSQL
type ConnParams map[ConnParamKey]string

// Set can add/update a key
func (cp ConnParams) Set(k ConnParamKey, v string) {
	cp[k] = v
}

// Get can retrieve a key/value pai
func (cp ConnParams) Get(k ConnParamKey) (v string) {
	return cp[k]
}

// Del can remove a key
func (cp ConnParams) Del(k ConnParamKey) {
	delete(cp, k)
}

// Isset returns true if the key is set
func (cp ConnParams) Isset(k ConnParamKey) bool {
	_, ok := cp[k]
	return ok
}

// Equals checks 2 ConnParams to be the same
func (cp ConnParams) Equals(cp2 ConnParams) bool {
	return reflect.DeepEqual(cp, cp2)
}

// Copy returns a shallow copy
func (cp ConnParams) Copy() ConnParams {
	ncp := ConnParams{}
	for k, v := range cp {
		ncp[k] = v
	}
	return ncp
}

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// ParseConnString parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func ParseConnString(name string) (ConnParams, error) {
	p := ConnParams{}
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return nil, fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			p.Set(ConnParamKey(ConnParamKey(keyRunes)), "")
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return nil, errors.New(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return nil, errors.New(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		p.Set(ConnParamKey(keyRunes), string(valRunes))
	}

	return p, nil
}

// URLToConnParams creates the connParams from the url.
func URLToConnParams(urlStr string) (ConnParams, error) {
	p := ConnParams{}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	if u.User != nil {
		v := u.User.Username()
		p.Set("user", v)
		v, _ = u.User.Password()
		p.Set("password", v)
	}

	i := strings.Index(u.Host, ":")
	if i < 0 {
		p.Set("host", u.Host)
	} else {
		p.Set("host", u.Host[:i])
		p.Set("port", u.Host[i+1:])
	}

	if u.Path != "" {
		p.Set("dbname", u.Path[1:])
	}

	q := u.Query()
	for k := range q {
		p.Set(ConnParamKey(k), q.Get(k))
	}

	return p, nil
}

// ConnString returns a connection string, its entries are sorted so the
// returned string can be reproducible and comparable
func (cp ConnParams) ConnString() string {
	var kvs []string
	escaper := strings.NewReplacer(` `, `\ `, `'`, `\'`, `\`, `\\`)
	for k, v := range cp {
		if v != "" {
			kvs = append(kvs, fmt.Sprintf("%s=%s", k, "="+escaper.Replace(v)))
		}
	}
	sort.Strings(kvs)
	return strings.Join(kvs, " ")
}

// WithUser returns a clone with the user fields set to the specified userName
func (cp ConnParams) WithUser(userName string) ConnParams {
	q := maps.Clone(cp)
	q[ConnParamKeyUser] = userName
	return q
}

// WithHost returns a clone with the host fields set to the specified hostName
func (cp ConnParams) WithHost(hostName string) ConnParams {
	q := maps.Clone(cp)
	q[ConnParamKeyHost] = hostName
	return q
}

// WithPort returns a clone with the port fields set as specified
func (cp ConnParams) WithPort(port uint) ConnParams {
	return cp.WithSPort(fmt.Sprintf("%d", port))
}

// WithSPort returns a clone with the port fields set as specified
func (cp ConnParams) WithSPort(port string) ConnParams {
	q := maps.Clone(cp)
	q[ConnParamKeyPort] = port
	return q
}

// WithAppName returns a clone with the port fields set as specified
func (cp ConnParams) WithAppName(appName string) ConnParams {
	q := maps.Clone(cp)
	q[ConnParamKeyAppName] = appName
	return q
}

// WithSSLMode returns a clone with the port fields set as specified
func (cp ConnParams) WithSSLMode(sslMode string) ConnParams {
	q := maps.Clone(cp)
	q[ConnParamKeySSLMode] = sslMode
	return q
}
