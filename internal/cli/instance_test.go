package cli

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/fikfikk-org/waapi/internal/store"
)

// TestCLIEditSetParsing memastikan flag --set mengonversi tipe bool, int, dan list ke JSON yang valid untuk InstanceSettings.
func TestCLIEditSetParsing(t *testing.T) {
	raw, err := parseEditSetFlags([]string{
		"context_store=true",
		"webhook_lanes=6",
		"optout_keywords=STOP,BERHENTI",
	})
	if err != nil {
		t.Fatalf("parseEditSetFlags gagal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal hasil parseEditSetFlags gagal: %v", err)
	}
	if v, ok := decoded["context_store"].(bool); !ok || !v {
		t.Fatalf("context_store diharapkan bool true, didapat %#v", decoded["context_store"])
	}
	if v, ok := decoded["webhook_lanes"].(float64); !ok || v != 6 {
		t.Fatalf("webhook_lanes diharapkan angka 6, didapat %#v", decoded["webhook_lanes"])
	}

	merged, err := store.Defaults().Merge(raw)
	if err != nil {
		t.Fatalf("Merge hasil --set ke InstanceSettings gagal: %v", err)
	}
	if !merged.ContextStore || merged.WebhookLanes != 6 {
		t.Fatalf("hasil merge tidak sesuai: %+v", merged)
	}
	if !reflect.DeepEqual(merged.OptoutKeywords, []string{"STOP", "BERHENTI"}) {
		t.Fatalf("optout_keywords tidak sesuai: %#v", merged.OptoutKeywords)
	}
}
