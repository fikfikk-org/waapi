package webhook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
)

const (
	webhookConsumerName = "webhook"
	deliverBatchLimit   = 32
	deliverMaxBytes     = 256 << 10 // 256 KiB per batch baca dari disk
	maxResponseBodyRead = 4 << 10   // baca respons penerima maksimal 4 KiB
	stopDrainTimeout    = 10 * time.Second
	commitFlushTimeout  = 3 * time.Second
)

type deliverOutcome uint8

const (
	outcomeDelivered deliverOutcome = iota
	outcomeDLQ
	outcomePaused
	outcomeInterrupted
)

// laneDeliverer menjalankan satu goroutine per lane milik satu instance aktif.
type laneDeliverer struct {
	svc        *Service
	instanceID string
	target     Target
	types      []string

	loopCtx    context.Context
	loopCancel context.CancelFunc
	reqCtx     context.Context
	reqCancel  context.CancelFunc

	wg sync.WaitGroup
}

func newLaneDeliverer(svc *Service, parentLoopCtx context.Context, instanceID string, target Target) *laneDeliverer {
	loopCtx, loopCancel := context.WithCancel(parentLoopCtx)
	// Context request dipisah agar Stop() bisa menunggu request in-flight <= 10s saat loopCtx sudah dibatalkan.
	reqCtx, reqCancel := context.WithCancel(context.Background())

	return &laneDeliverer{
		svc:        svc,
		instanceID: instanceID,
		target:     normalizeTarget(target),
		types:      parseCSVTypes(target.EventsCSV),
		loopCtx:    loopCtx,
		loopCancel: loopCancel,
		reqCtx:     reqCtx,
		reqCancel:  reqCancel,
	}
}

func (d *laneDeliverer) start() {
	lanes := d.target.Lanes
	if lanes <= 0 {
		lanes = 4
	}
	for lane := 0; lane < lanes; lane++ {
		d.wg.Add(1)
		go func(l int) {
			defer d.wg.Done()
			d.runLane(l, lanes)
		}(lane)
	}
}

func (d *laneDeliverer) stopLoop() {
	d.loopCancel()
}

func (d *laneDeliverer) hardCancel() {
	d.loopCancel()
	d.reqCancel()
}

func (d *laneDeliverer) wait() {
	d.wg.Wait()
	d.reqCancel()
	d.loopCancel()
}

func (d *laneDeliverer) runLane(lane, lanes int) {
	commitEvery := d.svc.opt.CommitEvery
	if commitEvery <= 0 {
		commitEvery = 16
	}
	commitInterval := d.svc.opt.CommitInterval
	if commitInterval <= 0 {
		commitInterval = time.Second
	}
	idleWait := d.svc.opt.IdleWait
	if idleWait <= 0 {
		idleWait = 30 * time.Second
	}

	cursor, err := d.svc.jr.Cursor(d.loopCtx, webhookConsumerName, d.instanceID, lane)
	if err != nil && d.loopCtx.Err() == nil {
		slog.Warn("gagal membaca cursor webhook awal", "instance", d.instanceID, "lane", lane, "err", err)
	}
	committed := cursor
	pendingEvents := 0
	lastCommit := time.Now()

	flushCursor := func(seq int64) {
		if seq <= committed {
			return
		}
		cCtx, cancel := context.WithTimeout(context.Background(), commitFlushTimeout)
		err := d.svc.jr.CommitCursor(cCtx, webhookConsumerName, d.instanceID, lane, seq)
		cancel()
		if err != nil {
			slog.Warn("gagal commit cursor webhook", "instance", d.instanceID, "lane", lane, "seq", seq, "err", err)
			return
		}
		committed = seq
		pendingEvents = 0
		lastCommit = time.Now()
	}

	subCh, unsub := d.svc.jr.Subscribe(d.instanceID)
	defer unsub()

	for {
		if d.loopCtx.Err() != nil {
			flushCursor(cursor)
			return
		}

		res, err := d.svc.jr.Read(d.loopCtx, journal.Query{
			InstanceID: d.instanceID,
			AfterSeq:   cursor,
			Types:      d.types,
			Lane:       lane,
			Lanes:      lanes,
			Limit:      deliverBatchLimit,
			MaxBytes:   deliverMaxBytes,
		})
		if err != nil {
			if d.loopCtx.Err() != nil {
				flushCursor(cursor)
				return
			}
			slog.Warn("gagal membaca journal webhook", "instance", d.instanceID, "lane", lane, "err", err)
			if !waitWithContext(d.loopCtx, time.Second) {
				flushCursor(cursor)
				return
			}
			continue
		}

		if len(res.Events) == 0 {
			if res.ScannedTo > cursor {
				cursor = res.ScannedTo
			}
			// Lane sepi atau semua event tersaring tipe tetap memajukan cursor sebelum menunggu.
			flushCursor(cursor)
			if res.HasMore {
				continue
			}
			if !waitForSignalOrTimer(d.loopCtx, subCh, idleWait) {
				flushCursor(cursor)
				return
			}
			continue
		}

		for idx, ev := range res.Events {
			if d.loopCtx.Err() != nil {
				flushCursor(cursor)
				return
			}

			if d.svc.gate != nil {
				deliver, gateErr := d.svc.gate.Wait(d.loopCtx, d.instanceID, ev.Seq)
				if gateErr != nil {
					if d.loopCtx.Err() != nil {
						flushCursor(cursor)
						return
					}
					slog.Warn("gate webhook mengembalikan error", "instance", d.instanceID, "seq", ev.Seq, "err", gateErr)
					if !waitWithContext(d.loopCtx, time.Second) {
						flushCursor(cursor)
						return
					}
					// Ulangi pembacaan dari posisi cursor saat ini.
					break
				}
				if !deliver {
					cursor = ev.Seq
					if idx == len(res.Events)-1 && res.ScannedTo > cursor {
						cursor = res.ScannedTo
					}
					pendingEvents++
					if pendingEvents >= commitEvery || time.Since(lastCommit) >= commitInterval {
						flushCursor(cursor)
					}
					continue
				}
			}

			outcome := d.deliverEventWithRetry(ev, &cursor, committed, flushCursor)
			switch outcome {
			case outcomeDelivered, outcomeDLQ:
				cursor = ev.Seq
				if idx == len(res.Events)-1 && res.ScannedTo > cursor {
					cursor = res.ScannedTo
				}
				pendingEvents++
				if pendingEvents >= commitEvery || time.Since(lastCommit) >= commitInterval {
					flushCursor(cursor)
				}
				if d.loopCtx.Err() != nil {
					flushCursor(cursor)
					return
				}
			case outcomePaused:
				// Jangan memajukan cursor melewati event 410 agar bisa dikirim ulang saat resume.
				flushCursor(cursor)
				return
			case outcomeInterrupted:
				flushCursor(cursor)
				return
			}
		}

		if !res.HasMore {
			if res.ScannedTo > cursor {
				cursor = res.ScannedTo
			}
			flushCursor(cursor)
			if !waitForSignalOrTimer(d.loopCtx, subCh, idleWait) {
				flushCursor(cursor)
				return
			}
		}
	}
}

func (d *laneDeliverer) deliverEventWithRetry(
	ev journal.Event,
	cursor *int64,
	committed int64,
	flushCursor func(int64),
) deliverOutcome {
	maxAttempts := d.target.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	attemptTimeout := d.svc.opt.AttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = 10 * time.Second
	}

	body, err := ev.MarshalEnvelope()
	if err != nil {
		slog.Warn("gagal marshal envelope journal", "instance", d.instanceID, "seq", ev.Seq, "err", err)
		d.recordDead(ev.Seq, 0, 0, err.Error())
		return outcomeDLQ
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if d.loopCtx.Err() != nil {
			return outcomeInterrupted
		}

		reqCtx, cancel := context.WithTimeout(d.reqCtx, attemptTimeout)
		status, retryAfter, hasRetryAfter, sendErr := sendWebhookHTTP(
			reqCtx, d.svc.client, d.target, d.instanceID, ev.Type, ev.Seq, attempt, time.Now(), body,
		)
		cancel()

		class := ClassifyResponse(status, sendErr)
		switch class {
		case ClassSuccess:
			return outcomeDelivered

		case ClassGone:
			slog.Error("webhook mengembalikan 410 Gone, pengiriman di-pause",
				"instance", d.instanceID, "seq", ev.Seq, "url", d.target.URL)
			if *cursor > committed {
				flushCursor(*cursor)
			}
			// Pastikan baris cursor tercatat aktif sebelum pause agar pruner tidak membuang event.
			cCtx, cCancel := context.WithTimeout(context.Background(), commitFlushTimeout)
			_ = d.svc.jr.CommitCursor(cCtx, webhookConsumerName, d.instanceID, 0, *cursor)
			_ = d.svc.jr.SetConsumerEnabled(cCtx, webhookConsumerName, d.instanceID, true)
			cCancel()

			pCtx, pCancel := context.WithTimeout(context.Background(), commitFlushTimeout)
			d.svc.pauseTarget(pCtx, d.instanceID, "gone")
			pCancel()

			d.stopLoop()
			return outcomePaused

		case ClassPermanent:
			capAttempts := min(maxAttempts, permanentMaxAttempts)
			if attempt >= capAttempts {
				errMsg := ""
				if sendErr != nil {
					errMsg = sendErr.Error()
				} else {
					errMsg = fmt.Sprintf("HTTP %d", status)
				}
				slog.Warn("event dipindah ke webhook_dead setelah error permanen",
					"instance", d.instanceID, "seq", ev.Seq, "status", status, "attempts", attempt)
				d.recordDead(ev.Seq, attempt, status, errMsg)
				return outcomeDLQ
			}

		case ClassTransient:
			if attempt >= maxAttempts {
				errMsg := ""
				if sendErr != nil {
					errMsg = sendErr.Error()
				} else {
					errMsg = fmt.Sprintf("HTTP %d", status)
				}
				slog.Warn("event dipindah ke webhook_dead setelah habis retry",
					"instance", d.instanceID, "seq", ev.Seq, "status", status, "attempts", attempt)
				d.recordDead(ev.Seq, attempt, status, errMsg)
				return outcomeDLQ
			}
		}

		// Jika loop dibatalkan saat HTTP sedang berjalan dan percobaan masih gagal, jangan sleep backoff.
		if d.loopCtx.Err() != nil {
			return outcomeInterrupted
		}

		waitDur := RetryDelay(attempt+1, nil)
		if class == ClassTransient && hasRetryAfter {
			waitDur = retryAfter
		}
		if waitDur > 0 && !waitWithContext(d.loopCtx, waitDur) {
			return outcomeInterrupted
		}
	}

	return outcomeInterrupted
}

func (d *laneDeliverer) recordDead(seq int64, attempts, status int, errMsg string) {
	if d.svc.dr == nil {
		return
	}
	cCtx, cancel := context.WithTimeout(context.Background(), commitFlushTimeout)
	defer cancel()
	if err := d.svc.dr.Record(cCtx, DeadEntry{
		InstanceID: d.instanceID,
		Seq:        seq,
		Attempts:   attempts,
		LastStatus: status,
		LastError:  errMsg,
		DeadAt:     time.Now().UnixMilli(),
	}); err != nil {
		slog.Warn("gagal mencatat event ke webhook_dead", "instance", d.instanceID, "seq", seq, "err", err)
	}
}

func sendWebhookHTTP(
	ctx context.Context,
	client *http.Client,
	target Target,
	instanceID, event string,
	seq int64,
	attempt int,
	now time.Time,
	body []byte,
) (status int, retryAfter time.Duration, hasRetryAfter bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		return 0, 0, false, err
	}
	ts := now.Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "waapi-webhook/2.0")

	if strings.EqualFold(target.Format, "standard") {
		whID := fmt.Sprintf("%s_%d", instanceID, seq)
		if seq <= 0 {
			whID = fmt.Sprintf("%s_test_%d", instanceID, ts)
		}
		req.Header.Set("webhook-id", whID)
		req.Header.Set("webhook-timestamp", strconv.FormatInt(ts, 10))
		if target.Secret != "" {
			sig, sigErr := SignStandard(target.Secret, whID, ts, body)
			if sigErr != nil {
				return 0, 0, false, sigErr
			}
			req.Header.Set("webhook-signature", sig)
		}
	} else {
		// Mode waapi (kompatibel v1 + header penambah id & nomor percobaan).
		req.Header.Set("X-WA-Event", event)
		req.Header.Set("X-WA-Instance", instanceID)
		req.Header.Set("X-WA-Timestamp", strconv.FormatInt(ts, 10))
		if seq > 0 {
			req.Header.Set("X-WA-Event-Id", strconv.FormatInt(seq, 10))
		}
		req.Header.Set("X-WA-Delivery-Attempt", strconv.Itoa(attempt))
		if target.Secret != "" {
			req.Header.Set("X-WA-Signature", Sign(target.Secret, ts, body))
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBodyRead))

	ra, ok := ParseRetryAfter(resp.Header.Get("Retry-After"), now)
	return resp.StatusCode, ra, ok, nil
}

func parseCSVTypes(csv string) []string {
	trimmed := strings.TrimSpace(csv)
	if trimmed == "" || trimmed == "*" {
		return nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			if t == "*" {
				return nil
			}
			out = append(out, t)
		}
	}
	return out
}

func waitWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitForSignalOrTimer(ctx context.Context, ch <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	case <-timer.C:
		return true
	}
}
