package wa

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fikfikk-org/waapi/internal/journal"
)

const maxVotersPerOption = 100

var (
	// ErrPollNotFound dikembalikan jika definisi polling tidak ditemukan pada (instance_id, chat, wa_id).
	ErrPollNotFound = errors.New("polling tidak ditemukan")
	// ErrPresenceSubLimit dikembalikan saat daftar langganan presence melebihi batas 100 JID per instance.
	ErrPresenceSubLimit = errors.New("batas maksimal 100 langganan presence tercapai")
)

// Poll merepresentasikan metadata polling masuk maupun keluar yang tersimpan di tabel polls (journal.db).
type Poll struct {
	InstanceID string
	Chat       string
	WAID       string
	Creator    string
	Name       string
	Options    []string
	Selectable int
	CreatedAt  int64
}

// PollVotePayload merepresentasikan struktur JSON event "poll_vote" di journal dan webhook v2.
type PollVotePayload struct {
	PollID         string   `json:"poll_id"`
	Chat           string   `json:"chat"`
	Voter          string   `json:"voter"`
	VoterPN        string   `json:"voter_pn,omitempty"`
	Selected       []string `json:"selected"`
	SelectedHashes []string `json:"selected_hashes"`
	PollKnown      bool     `json:"poll_known"`
	Timestamp      int64    `json:"timestamp"`
}

// PollOptionTally merangkum hitungan suara dan daftar pemilih untuk satu opsi polling.
type PollOptionTally struct {
	Name   string   `json:"name"`
	Votes  int      `json:"votes"`
	Voters []string `json:"voters"`
}

// PollTally merangkum agregasi seluruh suara aktif sebuah polling untuk endpoint REST v2.
type PollTally struct {
	PollID      string            `json:"poll_id"`
	Chat        string            `json:"chat"`
	Name        string            `json:"name"`
	Selectable  int               `json:"selectable"`
	Options     []PollOptionTally `json:"options"`
	TotalVoters int               `json:"total_voters"`
}

// PollStore mengelola persistensi definisi polling dan suara pemilih di journal.db.
type PollStore struct {
	db *sql.DB
}

// NewPollStore membuat PollStore yang terhubung ke database journal.db.
func NewPollStore(db *sql.DB) *PollStore {
	if db == nil {
		return nil
	}
	return &PollStore{db: db}
}

// SavePoll menyimpan definisi polling baru (khususnya sebelum mengirim polling keluar) dalam transaksi tersendiri.
func (s *PollStore) SavePoll(ctx context.Context, p Poll) error {
	if s == nil || s.db == nil {
		return errors.New("poll store belum siap")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mulai transaksi simpan poll: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.SavePollTx(ctx, tx, p); err != nil {
		return err
	}
	return tx.Commit()
}

// SavePollTx menyimpan definisi polling di dalam transaksi SQLite yang sedang berlangsung.
func (s *PollStore) SavePollTx(ctx context.Context, tx *sql.Tx, p Poll) error {
	if s == nil || tx == nil {
		return nil
	}
	optsJSON, err := json.Marshal(p.Options)
	if err != nil {
		return fmt.Errorf("marshal opsi polling: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO polls (instance_id, chat, wa_id, creator, name, options, selectable, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id, chat, wa_id) DO NOTHING`,
		p.InstanceID, p.Chat, p.WAID, p.Creator, p.Name, string(optsJSON), p.Selectable, p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("simpan definisi poll: %w", err)
	}
	return nil
}

// RecordVoteAndAppend memetakan hash suara polling, memperbarui poll_votes, dan menyisipkan event poll_vote dalam satu transaksi atomik.
func (s *PollStore) RecordVoteAndAppend(
	ctx context.Context,
	j *journal.Journal,
	ev *journal.Event,
	pollID, voterPN string,
	selectedHashes [][]byte,
	outPayload *[]byte,
) (journal.AppendResult, error) {
	if s == nil || s.db == nil || j == nil || ev == nil {
		return journal.AppendResult{}, errors.New("poll store atau journal belum siap")
	}

	var res journal.AppendResult
	err := j.RunTx(ctx, func(txCtx context.Context, tx *sql.Tx, _ int64) error {
		payload, pErr := s.applyVoteTx(txCtx, tx, ev.InstanceID, ev.Chat, pollID, ev.Sender, voterPN, selectedHashes, ev.TS)
		if pErr != nil {
			return pErr
		}
		ev.Payload = payload
		if outPayload != nil {
			*outPayload = payload
		}
		appRes, aErr := journal.InsertTx(txCtx, tx, ev)
		if aErr != nil {
			return aErr
		}
		res = appRes
		ev.Seq = appRes.Seq
		return nil
	})
	if err != nil {
		return journal.AppendResult{}, err
	}
	j.Notify(ev.InstanceID)
	return res, nil
}

func (s *PollStore) applyVoteTx(
	ctx context.Context,
	tx *sql.Tx,
	instanceID, chat, pollID, voter, voterPN string,
	selectedHashes [][]byte,
	ts int64,
) ([]byte, error) {
	var rawOpts string
	err := tx.QueryRowContext(ctx, `
		SELECT options FROM polls WHERE instance_id = ? AND chat = ? AND wa_id = ?`,
		instanceID, chat, pollID,
	).Scan(&rawOpts)

	selected := make([]string, 0)
	selectedHex := make([]string, 0, len(selectedHashes))
	pollKnown := false

	switch {
	case err == nil:
		pollKnown = true
		var optNames []string
		if uErr := json.Unmarshal([]byte(rawOpts), &optNames); uErr != nil {
			return nil, fmt.Errorf("decode opsi polling tersimpan: %w", uErr)
		}
		hashToName := make(map[string]string, len(optNames))
		for _, opt := range optNames {
			sum := sha256.Sum256([]byte(opt))
			hashToName[string(sum[:])] = opt
		}
		for _, h := range selectedHashes {
			if name, ok := hashToName[string(h)]; ok {
				selected = append(selected, name)
			} else if len(h) > 0 {
				selectedHex = append(selectedHex, hex.EncodeToString(h))
			}
		}

		voterKey := voter
		if voterPN != "" {
			voterKey = voterPN
		}
		selJSON, mErr := json.Marshal(selected)
		if mErr != nil {
			return nil, mErr
		}
		if _, uErr := tx.ExecContext(ctx, `
			INSERT INTO poll_votes (instance_id, chat, wa_id, voter, selected, voted_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(instance_id, chat, wa_id, voter)
			DO UPDATE SET selected = excluded.selected, voted_at = excluded.voted_at`,
			instanceID, chat, pollID, voterKey, string(selJSON), ts,
		); uErr != nil {
			return nil, fmt.Errorf("simpan suara polling: %w", uErr)
		}

	case errors.Is(err, sql.ErrNoRows):
		for _, h := range selectedHashes {
			if len(h) > 0 {
				selectedHex = append(selectedHex, hex.EncodeToString(h))
			}
		}

	default:
		return nil, fmt.Errorf("baca definisi polling: %w", err)
	}

	payload, err := json.Marshal(PollVotePayload{
		PollID:         pollID,
		Chat:           chat,
		Voter:          voter,
		VoterPN:        voterPN,
		Selected:       selected,
		SelectedHashes: selectedHex,
		PollKnown:      pollKnown,
		Timestamp:      ts,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload poll_vote: %w", err)
	}
	return payload, nil
}

// GetTally mengembalikan rekap jumlah pemilih dan daftar pemilih per opsi (maks 100 pemilih per opsi).
func (s *PollStore) GetTally(ctx context.Context, instanceID, chat, waID string) (*PollTally, error) {
	if s == nil || s.db == nil {
		return nil, ErrPollNotFound
	}

	var (
		name       string
		rawOpts    string
		selectable int
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT name, options, selectable
		FROM polls
		WHERE instance_id = ? AND chat = ? AND wa_id = ?`,
		instanceID, chat, waID,
	).Scan(&name, &rawOpts, &selectable)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPollNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("baca definisi polling: %w", err)
	}

	var optNames []string
	if err := json.Unmarshal([]byte(rawOpts), &optNames); err != nil {
		return nil, fmt.Errorf("decode opsi polling: %w", err)
	}

	optIndex := make(map[string]int, len(optNames))
	tallies := make([]PollOptionTally, len(optNames))
	for idx, opt := range optNames {
		optIndex[opt] = idx
		tallies[idx] = PollOptionTally{
			Name:   opt,
			Voters: make([]string, 0),
		}
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT voter, selected
		FROM poll_votes
		WHERE instance_id = ? AND chat = ? AND wa_id = ?
		ORDER BY voted_at ASC, voter ASC`,
		instanceID, chat, waID,
	)
	if err != nil {
		return nil, fmt.Errorf("baca suara polling: %w", err)
	}
	defer rows.Close()

	totalVoters := 0
	for rows.Next() {
		var (
			voter  string
			rawSel string
		)
		if err := rows.Scan(&voter, &rawSel); err != nil {
			return nil, fmt.Errorf("scan suara polling: %w", err)
		}
		var chosen []string
		if err := json.Unmarshal([]byte(rawSel), &chosen); err != nil {
			return nil, fmt.Errorf("decode pilihan polling: %w", err)
		}
		if len(chosen) == 0 {
			continue
		}
		totalVoters++
		for _, ch := range chosen {
			idx, ok := optIndex[ch]
			if !ok {
				continue
			}
			tallies[idx].Votes++
			if len(tallies[idx].Voters) < maxVotersPerOption {
				tallies[idx].Voters = append(tallies[idx].Voters, voter)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterasi suara polling: %w", err)
	}

	return &PollTally{
		PollID:      waID,
		Chat:        chat,
		Name:        name,
		Selectable:  selectable,
		Options:     tallies,
		TotalVoters: totalVoters,
	}, nil
}
