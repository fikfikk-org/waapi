package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

func openTestAuthStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatalf("store.Open gagal: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestKeyBaru_ScopeDefault memastikan CreateWithOptions tanpa scope menghasilkan set default §4.1.5 serta default MCP policy bila mengandung scope mcp.
func TestKeyBaru_ScopeDefault(t *testing.T) {
	ctx := context.Background()
	st := openTestAuthStore(t)
	mgr, err := NewManager(ctx, st)
	if err != nil {
		t.Fatalf("NewManager gagal: %v", err)
	}

	raw, meta, err := mgr.CreateWithOptions(ctx, CreateKeyRequest{
		Name: "dash-default",
	})
	if err != nil {
		t.Fatalf("CreateWithOptions gagal: %v", err)
	}
	if meta.Scopes != DefaultScopes().String() {
		t.Fatalf("scopes tersimpan diharapkan %q, didapat %q", DefaultScopes().String(), meta.Scopes)
	}

	km, err := mgr.Verify(raw)
	if err != nil {
		t.Fatalf("Verify key baru gagal: %v", err)
	}
	for _, want := range []Scope{
		ScopeInstancesRead,
		ScopeMessagesSend,
		ScopeMessagesRead,
		ScopeMediaWrite,
		ScopeEventsRead,
		ScopeHealthRead,
	} {
		if !km.Scopes.Has(want) {
			t.Fatalf("key baru default wajib memiliki scope %q", want)
		}
	}
	for _, forbid := range []Scope{ScopePair, ScopeSettingsAdmin, ScopeGroupsWrite, ScopeMCP, ScopeHooksAdmin} {
		if km.Scopes.Has(forbid) {
			t.Fatalf("key baru default tidak boleh memiliki scope %q", forbid)
		}
	}
	if km.Policy.FirstContact != "allow" || km.Policy.Destructive != "allow" {
		t.Fatalf("policy default non-MCP tidak sesuai: %+v", km.Policy)
	}

	// Key ber-scope mcp tanpa policy eksplisit wajib mendapat DefaultMCPPolicy()
	rawMCP, _, err := mgr.CreateWithOptions(ctx, CreateKeyRequest{
		Name:   "agent-mcp",
		Scopes: ParseMustScopes(t, "mcp,messages:send"),
	})
	if err != nil {
		t.Fatalf("CreateWithOptions mcp gagal: %v", err)
	}
	kmMCP, err := mgr.Verify(rawMCP)
	if err != nil {
		t.Fatalf("Verify mcp gagal: %v", err)
	}
	if kmMCP.Policy.ReplyWindow != store.Duration(24*time.Hour) ||
		kmMCP.Policy.FirstContact != "ask" ||
		kmMCP.Policy.MaxSendsPerHour != 30 ||
		kmMCP.Policy.Destructive != "ask" {
		t.Fatalf("policy default MCP tidak sesuai: %+v", kmMCP.Policy)
	}
}

func ParseMustScopes(t *testing.T, csv string) ScopeSet {
	t.Helper()
	s, err := ParseScopes(csv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestKeyLama_BintangSemuaScope memastikan key v1 maupun baris DB dengan scopes='*' lolos seluruh scope saat dimuat ulang.
func TestKeyLama_BintangSemuaScope(t *testing.T) {
	ctx := context.Background()
	st := openTestAuthStore(t)
	mgr, err := NewManager(ctx, st)
	if err != nil {
		t.Fatal(err)
	}

	// Create() versi v1 wajib tetap menghasilkan scope '*'
	rawV1, metaV1, err := mgr.Create(ctx, "legacy-v1", "")
	if err != nil {
		t.Fatalf("Create v1 gagal: %v", err)
	}
	if metaV1.Scopes != "*" {
		t.Fatalf("Create v1 harus berscope '*', didapat %q", metaV1.Scopes)
	}

	// Simulasikan restart daemon dengan Reload() dari app.db
	if err := mgr.Reload(ctx); err != nil {
		t.Fatalf("Reload gagal: %v", err)
	}
	km, err := mgr.Verify(rawV1)
	if err != nil {
		t.Fatalf("Verify v1 pasca-Reload gagal: %v", err)
	}
	for _, sc := range []Scope{
		ScopeInstancesRead, ScopePair, ScopeMessagesSend, ScopeMessagesRead,
		ScopeMediaWrite, ScopeEventsRead, ScopeGroupsRead, ScopeGroupsWrite,
		ScopeHealthRead, ScopeSettingsAdmin, ScopeHooksAdmin, ScopeMCP,
	} {
		if !km.Scopes.Has(sc) {
			t.Fatalf("key '*' seharusnya mencakup scope %q", sc)
		}
	}
}

// TestKeyRotateGrace memverifikasi rotasi API key menjaga atribut sama dan key lama tetap aktif hingga grace habis.
func TestKeyRotateGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		st := openTestAuthStore(t)
		mgr, err := NewManager(ctx, st)
		if err != nil {
			t.Fatal(err)
		}

		customPolicy := DefaultPolicy()
		customPolicy.MaxSendsPerHour = 12
		customScopes := ParseMustScopes(t, "instances:read,messages:send,pair")

		oldRaw, oldMeta, err := mgr.CreateWithOptions(ctx, CreateKeyRequest{
			Name:       "ops-key",
			Scopes:     customScopes,
			RatePerSec: 15,
			Policy:     customPolicy,
		})
		if err != nil {
			t.Fatalf("CreateWithOptions gagal: %v", err)
		}

		newRaw, newMeta, err := mgr.Rotate(ctx, oldMeta.ID, 24*time.Hour)
		if err != nil {
			t.Fatalf("Rotate gagal: %v", err)
		}
		if newMeta.ID == oldMeta.ID || newRaw == oldRaw {
			t.Fatal("key hasil rotasi harus memiliki ID dan token baru yang berbeda")
		}

		// Key baru mewarisi scope, rate limit, dan policy yang sama
		newKM, err := mgr.Verify(newRaw)
		if err != nil {
			t.Fatalf("Verify key baru gagal: %v", err)
		}
		if newKM.Scopes != customScopes || newKM.RatePerSec != 15 || newKM.Policy.MaxSendsPerHour != 12 {
			t.Fatalf("atribut key baru tidak mewarisi key lama: scopes=%v rate=%d policy=%+v",
				newKM.Scopes, newKM.RatePerSec, newKM.Policy)
		}

		// Tepat sebelum grace 24h habis -> key lama masih valid
		time.Sleep(23*time.Hour + 59*time.Minute)
		synctest.Wait()
		if _, err := mgr.Verify(oldRaw); err != nil {
			t.Fatalf("key lama seharusnya masih aktif di dalam masa grace, didapat %v", err)
		}

		// Setelah grace 24h terlewati -> key lama mengembalikan ErrKeyExpired, key baru tetap aktif
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if _, err := mgr.Verify(oldRaw); !errors.Is(err, ErrKeyExpired) {
			t.Fatalf("key lama setelah grace seharusnya ErrKeyExpired, didapat %v", err)
		}
		if _, err := mgr.Verify(newRaw); err != nil {
			t.Fatalf("key baru seharusnya tetap aktif setelah grace key lama habis, didapat %v", err)
		}
	})
}

// TestAuditLog_WriteListPrune memverifikasi pencatatan, filter pola action/since, dan pruning retensi 90 hari audit_log.
func TestAuditLog_WriteListPrune(t *testing.T) {
	ctx := context.Background()
	st := openTestAuthStore(t)
	al := NewAuditLog(st.DB)

	if err := al.Write(ctx, "control:uid=1000", "key.create", "k_111111", "name=dash"); err != nil {
		t.Fatalf("Write audit 1 gagal: %v", err)
	}
	if err := al.Write(ctx, "key:k_111111", "settings.update", "sales", `{"recycle_at":"04:00"}`); err != nil {
		t.Fatalf("Write audit 2 gagal: %v", err)
	}
	// Sisipkan baris tua > 90 hari untuk uji Prune
	oldAt := time.Now().Add(-95 * 24 * time.Hour).Unix()
	if _, err := st.DB.ExecContext(ctx,
		`INSERT INTO audit_log (at, actor, action, target, detail) VALUES (?, 'control:uid=0', 'key.revoke', 'k_old', '')`,
		oldAt,
	); err != nil {
		t.Fatal(err)
	}

	recent, err := al.List(ctx, AuditQuery{
		Since:         time.Now().Add(-time.Hour),
		ActionPattern: "key.*",
		Limit:         50,
	})
	if err != nil {
		t.Fatalf("List audit gagal: %v", err)
	}
	if len(recent) != 1 || recent[0].Action != "key.create" || recent[0].Actor != "control:uid=1000" {
		t.Fatalf("hasil List audit terfilter tidak sesuai: %+v", recent)
	}

	deleted, err := al.Prune(ctx, time.Now().Add(-90*24*time.Hour))
	if err != nil {
		t.Fatalf("Prune audit gagal: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("Prune diharapkan menghapus 1 baris tua, didapat %d", deleted)
	}
}
