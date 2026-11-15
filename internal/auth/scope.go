package auth

import (
	"errors"
	"fmt"
	"strings"
)

// Scope merepresentasikan satu izin fungsional pada API publik dan MCP.
type Scope string

const (
	ScopeInstancesRead Scope = "instances:read"
	ScopePair          Scope = "pair"
	ScopeMessagesSend  Scope = "messages:send"
	ScopeMessagesRead  Scope = "messages:read"
	ScopeMediaWrite    Scope = "media:write"
	ScopeEventsRead    Scope = "events:read"
	ScopeGroupsRead    Scope = "groups:read"
	ScopeGroupsWrite   Scope = "groups:write"
	ScopeHealthRead    Scope = "health:read"
	ScopeSettingsAdmin Scope = "settings:admin"
	ScopeHooksAdmin    Scope = "hooks:admin"
	ScopeMCP           Scope = "mcp"
	ScopeWildcard      Scope = "*"
)

// ScopeSet adalah bitmask ringkas untuk kumpulan Scope milik sebuah API key.
type ScopeSet uint32

var scopeOrder = []Scope{
	ScopeInstancesRead,
	ScopePair,
	ScopeMessagesSend,
	ScopeMessagesRead,
	ScopeMediaWrite,
	ScopeEventsRead,
	ScopeGroupsRead,
	ScopeGroupsWrite,
	ScopeHealthRead,
	ScopeSettingsAdmin,
	ScopeHooksAdmin,
	ScopeMCP,
}

var scopeBits = func() map[Scope]ScopeSet {
	m := make(map[Scope]ScopeSet, len(scopeOrder))
	for i, s := range scopeOrder {
		m[s] = ScopeSet(1 << i)
	}
	return m
}()

// ScopeAll merepresentasikan seluruh scope yang tersedia (setara '*').
const ScopeAll ScopeSet = (1 << 12) - 1

// Has memeriksa apakah ScopeSet memiliki scope yang diminta.
func (s ScopeSet) Has(target Scope) bool {
	if target == ScopeWildcard {
		return s == ScopeAll
	}
	bit, ok := scopeBits[target]
	if !ok {
		return false
	}
	return s&bit != 0
}

// String mengonversi ScopeSet menjadi representasi CSV atau "*".
func (s ScopeSet) String() string {
	if s == ScopeAll {
		return string(ScopeWildcard)
	}
	var out []string
	for _, sc := range scopeOrder {
		if s.Has(sc) {
			out = append(out, string(sc))
		}
	}
	return strings.Join(out, ",")
}

// ParseScopes mem-parse daftar scope dipisah koma (atau "*") menjadi ScopeSet.
func ParseScopes(csv string) (ScopeSet, error) {
	trimmed := strings.TrimSpace(csv)
	if trimmed == "" {
		return 0, errors.New("daftar scope tidak boleh kosong")
	}

	var out ScopeSet
	for _, part := range strings.Split(trimmed, ",") {
		tok := Scope(strings.TrimSpace(part))
		if tok == "" {
			return 0, errors.New("daftar scope mengandung elemen kosong")
		}
		if tok == ScopeWildcard {
			out |= ScopeAll
			continue
		}
		bit, ok := scopeBits[tok]
		if !ok {
			return 0, fmt.Errorf("scope tidak dikenal: %q", tok)
		}
		out |= bit
	}
	return out, nil
}
