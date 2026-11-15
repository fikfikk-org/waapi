package auth

import "testing"

// TestParseScopes memastikan parsing CSV scope, penolakan scope asing, dan wildcard '*' berjalan benar.
func TestParseScopes(t *testing.T) {
	set, err := ParseScopes("instances:read,messages:send")
	if err != nil {
		t.Fatalf("ParseScopes valid gagal: %v", err)
	}
	if !set.Has(ScopeInstancesRead) || !set.Has(ScopeMessagesSend) {
		t.Fatalf("scope yang di-parse seharusnya aktif: %v", set)
	}
	if set.Has(ScopeMCP) {
		t.Fatal("scope mcp seharusnya tidak aktif")
	}

	if _, err := ParseScopes("instances:read,scope_asing"); err == nil {
		t.Fatal("scope asing seharusnya ditolak ParseScopes")
	}

	all, err := ParseScopes("*")
	if err != nil {
		t.Fatalf("ParseScopes('*') gagal: %v", err)
	}
	if all != ScopeAll {
		t.Fatalf("ParseScopes('*') diharapkan sama dengan ScopeAll (%d), didapat %d", ScopeAll, all)
	}
	for _, sc := range []Scope{
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
	} {
		if !all.Has(sc) {
			t.Fatalf("ScopeAll seharusnya mencakup %q", sc)
		}
	}
}
