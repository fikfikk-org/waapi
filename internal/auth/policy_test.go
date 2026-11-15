package auth

import (
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/store"
)

// TestKeyPolicy_Validate memastikan default policy, penolakan enum salah, dan penolakan glob tidak valid.
func TestKeyPolicy_Validate(t *testing.T) {
	if err := DefaultPolicy().Validate(); err != nil {
		t.Fatalf("DefaultPolicy harus valid: %v", err)
	}

	mcp := DefaultMCPPolicy()
	if err := mcp.Validate(); err != nil {
		t.Fatalf("DefaultMCPPolicy harus valid: %v", err)
	}
	if mcp.ReplyWindow != store.Duration(24*time.Hour) {
		t.Fatalf("DefaultMCPPolicy().ReplyWindow diharapkan 24h, didapat %v", mcp.ReplyWindow)
	}

	badFirst := DefaultPolicy()
	badFirst.FirstContact = "maybe"
	if err := badFirst.Validate(); err == nil {
		t.Fatal("nilai enum first_contact tidak valid seharusnya ditolak")
	}

	badDestructive := DefaultPolicy()
	badDestructive.Destructive = "skip"
	if err := badDestructive.Validate(); err == nil {
		t.Fatal("nilai enum destructive tidak valid seharusnya ditolak")
	}

	badGlob := DefaultPolicy()
	badGlob.AllowedChats = []string{"[628"}
	if err := badGlob.Validate(); err == nil {
		t.Fatal("pola glob allowed_chats yang rusak seharusnya ditolak")
	}
}
