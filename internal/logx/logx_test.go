package logx

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type capturedRecord struct {
	level  slog.Level
	msg    string
	module string
}

type captureHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	var mod string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "module" {
			mod = a.Value.String()
		}
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, capturedRecord{
		level:  r.Level,
		msg:    r.Message,
		module: mod,
	})
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

// TestWALogger_FilterLevelDanRateLimit memastikan Debugf dibuang pada level warn dan flood 1000 pesan/detik dibatasi dengan ringkasan.
func TestWALogger_FilterLevelDanRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &captureHandler{}
		base := slog.New(h)
		wlog := WALogger(base, "wa", slog.LevelWarn)
		sub := wlog.Sub("client")

		sub.Debugf("pesan debug yang harus dibuang %d", 1)
		sub.Infof("pesan info yang juga harus dibuang %d", 2)

		h.mu.Lock()
		before := len(h.records)
		h.mu.Unlock()
		if before != 0 {
			t.Fatalf("debug dan info seharusnya dibuang pada level warn, tercatat %d", before)
		}

		for i := 0; i < 1000; i++ {
			sub.Warnf("peringatan ke-%d", i)
			time.Sleep(time.Millisecond)
		}

		h.mu.Lock()
		recs := append([]capturedRecord(nil), h.records...)
		h.mu.Unlock()

		var normalCount, summaryCount int
		for _, rec := range recs {
			if strings.Contains(rec.msg, "baris log whatsmeow dibuang karena rate limit") {
				summaryCount++
				if rec.module == "" {
					t.Fatal("ringkasan rate limit harus menyertakan atribut module")
				}
				continue
			}
			normalCount++
			if rec.module == "" {
				t.Fatal("log whatsmeow harus menyertakan atribut module")
			}
		}
		if normalCount > 70 {
			t.Fatalf("jumlah record normal melebihi batas rate limit (<= 70): %d", normalCount)
		}
		if summaryCount != 1 {
			t.Fatalf("diharapkan tepat 1 ringkasan rate limit, didapat %d", summaryCount)
		}
	})
}
