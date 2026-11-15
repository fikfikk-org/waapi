package wa

import (
	"context"

	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// LIDResolver memetakan JID LID (@lid) menjadi JID nomor telepon kanonik (@s.whatsapp.net).
// Implementasi harus aman dipanggil di hot path tanpa menyentuh jaringan atau app.db.
type LIDResolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool)
}

// storeLIDResolver membungkus store.LIDStore milik whatsmeow untuk resolusi LID lokal berbasis memori + wa.db.
type storeLIDResolver struct {
	lids waStore.LIDStore
}

// newStoreLIDResolver membuat LIDResolver dari store.LIDStore milik whatsmeow.Device.
func newStoreLIDResolver(lids waStore.LIDStore) LIDResolver {
	if lids == nil {
		return nil
	}
	return &storeLIDResolver{lids: lids}
}

// PNForLID mengembalikan JID PN yang dipetakan untuk LID jika tersedia di cache atau tabel LID wa.db.
func (r *storeLIDResolver) PNForLID(ctx context.Context, lid types.JID) (types.JID, bool) {
	if r == nil || r.lids == nil || lid.IsEmpty() {
		return types.EmptyJID, false
	}
	pn, err := r.lids.GetPNForLID(ctx, lid.ToNonAD())
	if err != nil || pn.IsEmpty() {
		return types.EmptyJID, false
	}
	return pn, true
}
