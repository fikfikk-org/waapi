package wa

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/fikfikk-org/waapi/internal/jid"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const (
	checkNumbersCacheCap   = 2048
	checkNumbersCacheTTL   = 24 * time.Hour
	contactProfileCacheCap = 256
	contactProfileCacheTTL = time.Hour
)

// ContactPicture menyimpan URL dan ID foto profil kontak WhatsApp.
type ContactPicture struct {
	URL string `json:"url"`
	ID  string `json:"id"`
}

// ContactProfile merangkum profil lengkap kontak beserta pemetaan PN <-> LID.
type ContactProfile struct {
	JID          string          `json:"jid"`
	PN           string          `json:"pn"`
	LID          string          `json:"lid"`
	PushName     string          `json:"push_name"`
	FullName     string          `json:"full_name"`
	BusinessName string          `json:"business_name"`
	About        string          `json:"about"`
	Picture      *ContactPicture `json:"picture"`
	VerifiedName string          `json:"verified_name"`
}

type contactClient interface {
	IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error)
	GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error)
	GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error)
	GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error)
	GetUserInfo(ctx context.Context, jids []types.JID) (map[types.JID]types.UserInfo, error)
	GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
}

type whatsmeowContactClient struct {
	cli *whatsmeow.Client
}

func (w whatsmeowContactClient) IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	return w.cli.IsOnWhatsApp(ctx, phones)
}

func (w whatsmeowContactClient) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	if w.cli.Store == nil || w.cli.Store.Contacts == nil {
		return types.ContactInfo{}, nil
	}
	return w.cli.Store.Contacts.GetContact(ctx, user)
}

func (w whatsmeowContactClient) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	if w.cli.Store == nil || w.cli.Store.LIDs == nil {
		return types.EmptyJID, nil
	}
	return w.cli.Store.LIDs.GetPNForLID(ctx, lid)
}

func (w whatsmeowContactClient) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	if w.cli.Store == nil || w.cli.Store.LIDs == nil {
		return types.EmptyJID, nil
	}
	return w.cli.Store.LIDs.GetLIDForPN(ctx, pn)
}

func (w whatsmeowContactClient) GetUserInfo(ctx context.Context, jids []types.JID) (map[types.JID]types.UserInfo, error) {
	return w.cli.GetUserInfo(ctx, jids)
}

func (w whatsmeowContactClient) GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	return w.cli.GetProfilePictureInfo(ctx, jid, params)
}

func (i *Instance) ensureCheckLRU() *LRU[string, NumberCheckResult] {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.checkLRU == nil {
		i.checkLRU = NewLRU[string, NumberCheckResult](checkNumbersCacheCap, checkNumbersCacheTTL)
	}
	return i.checkLRU
}

func (i *Instance) ensureContactLRU() *LRU[string, ContactProfile] {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.contactLRU == nil {
		i.contactLRU = NewLRU[string, ContactProfile](contactProfileCacheCap, contactProfileCacheTTL)
	}
	return i.contactLRU
}

func (i *Instance) activeContactClient() (contactClient, error) {
	if i == nil {
		return nil, ErrNotConnected
	}
	if i.contactCli != nil {
		return i.contactCli, nil
	}
	cli := i.getClient()
	if cli == nil || !cli.IsConnected() {
		return nil, ErrNotConnected
	}
	return whatsmeowContactClient{cli: cli}, nil
}

// GetContactProfile mengambil profil kontak (nama, status about, foto profil, verified name, PN/LID) dengan LRU 256 entri TTL 1 jam.
func (i *Instance) GetContactProfile(ctx context.Context, rawJID string) (ContactProfile, error) {
	parsed, err := jid.ParseRecipient(rawJID)
	if err != nil {
		return ContactProfile{}, err
	}
	target := parsed.ToNonAD()
	if target.Server != types.DefaultUserServer && target.Server != types.HiddenUserServer {
		return ContactProfile{}, NewGroupError(http.StatusBadRequest, "invalid_request", "JID harus berupa akun pengguna (PN atau LID)", 0)
	}

	cache := i.ensureContactLRU()
	cacheKey := target.String()
	if cached, ok := cache.Get(cacheKey); ok {
		return cached, nil
	}

	ch := i.contactGroup.DoChan(cacheKey, func() (any, error) {
		if cached, ok := cache.Get(cacheKey); ok {
			return cached, nil
		}
		cli, err := i.activeContactClient()
		if err != nil {
			return ContactProfile{}, err
		}
		sfCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		prof, err := fetchContactProfile(sfCtx, cli, target)
		if err != nil {
			return ContactProfile{}, err
		}
		cache.Set(cacheKey, prof)
		return prof, nil
	})

	select {
	case <-ctx.Done():
		return ContactProfile{}, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return ContactProfile{}, res.Err
		}
		return res.Val.(ContactProfile), nil
	}
}

func fetchContactProfile(ctx context.Context, cli contactClient, target types.JID) (ContactProfile, error) {
	var pn, lid types.JID
	switch target.Server {
	case types.HiddenUserServer:
		lid = target
		if resolved, err := cli.GetPNForLID(ctx, lid); err == nil && !resolved.IsEmpty() {
			nonAD := resolved.ToNonAD()
			if nonAD.Server == types.DefaultUserServer {
				pn = nonAD
			}
		}
	case types.DefaultUserServer:
		pn = target
		if resolved, err := cli.GetLIDForPN(ctx, pn); err == nil && !resolved.IsEmpty() {
			nonAD := resolved.ToNonAD()
			if nonAD.Server == types.HiddenUserServer {
				lid = nonAD
			}
		}
	}

	// Jika input @lid sudah memiliki PN lokal, prioritaskan PN tersebut saat memanggil GetUserInfo & foto profil.
	queryJID := target
	if !pn.IsEmpty() {
		queryJID = pn
	}

	prof := ContactProfile{
		JID: target.String(),
		PN:  jidStringOrEmpty(pn),
		LID: jidStringOrEmpty(lid),
	}

	cInfo, _ := cli.GetContact(ctx, target)
	if !cInfo.Found && queryJID != target {
		if altInfo, err := cli.GetContact(ctx, queryJID); err == nil && altInfo.Found {
			cInfo = altInfo
		}
	}
	if cInfo.Found {
		prof.PushName = strings.TrimSpace(cInfo.PushName)
		prof.FullName = strings.TrimSpace(cInfo.FullName)
		if prof.FullName == "" {
			prof.FullName = strings.TrimSpace(cInfo.FirstName)
		}
		prof.BusinessName = strings.TrimSpace(cInfo.BusinessName)
	}

	userInfos, err := cli.GetUserInfo(ctx, []types.JID{queryJID})
	if err != nil {
		return ContactProfile{}, MapGroupIQError(err)
	}
	if uInfo, ok := lookupUserInfo(userInfos, queryJID, target); ok {
		prof.About = uInfo.Status
		if prof.LID == "" && !uInfo.LID.IsEmpty() {
			prof.LID = uInfo.LID.ToNonAD().String()
		}
		if uInfo.VerifiedName != nil && uInfo.VerifiedName.Details != nil {
			prof.VerifiedName = strings.TrimSpace(uInfo.VerifiedName.Details.GetVerifiedName())
		}
	}

	pic, err := cli.GetProfilePictureInfo(ctx, queryJID, &whatsmeow.GetProfilePictureParams{})
	if err != nil {
		if !isMissingOrPrivatePictureErr(err) {
			return ContactProfile{}, MapGroupIQError(err)
		}
	} else if pic != nil && pic.URL != "" {
		prof.Picture = &ContactPicture{
			URL: pic.URL,
			ID:  pic.ID,
		}
	}

	return prof, nil
}

func lookupUserInfo(m map[types.JID]types.UserInfo, primary, fallback types.JID) (types.UserInfo, bool) {
	if len(m) == 0 {
		return types.UserInfo{}, false
	}
	if info, ok := m[primary]; ok {
		return info, true
	}
	if info, ok := m[fallback]; ok {
		return info, true
	}
	for k, v := range m {
		if k.ToNonAD() == primary || k.ToNonAD() == fallback {
			return v, true
		}
	}
	return types.UserInfo{}, false
}

func isMissingOrPrivatePictureErr(err error) bool {
	if errors.Is(err, whatsmeow.ErrProfilePictureNotSet) || errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized) {
		return true
	}
	var iqErr *whatsmeow.IQError
	if errors.As(err, &iqErr) {
		return iqErr.Code == 401 || iqErr.Code == 404
	}
	return false
}

func jidStringOrEmpty(j types.JID) string {
	if j.IsEmpty() {
		return ""
	}
	return j.ToNonAD().String()
}

// GetContactProfile mengambil profil kontak pada instance terkait.
func (m *Manager) GetContactProfile(ctx context.Context, instanceID, rawJID string) (ContactProfile, error) {
	inst, ok := m.Get(instanceID)
	if !ok {
		return ContactProfile{}, store.ErrNotFound
	}
	return inst.GetContactProfile(ctx, rawJID)
}
