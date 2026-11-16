package wa

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type fakeContactClient struct {
	mu           sync.Mutex
	isOnWAHits   atomic.Int32
	userInfoHits atomic.Int32
	picHits      atomic.Int32
	gate         chan struct{}

	contacts map[string]types.ContactInfo
	lidToPN  map[string]types.JID
	pnToLID  map[string]types.JID
	userInfo map[types.JID]types.UserInfo
	pics     map[string]*types.ProfilePictureInfo
	picErr   map[string]error
	waRespFn func(phones []string) []types.IsOnWhatsAppResponse
}

func (f *fakeContactClient) IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.isOnWAHits.Add(1)
	if f.waRespFn != nil {
		return f.waRespFn(phones), nil
	}
	out := make([]types.IsOnWhatsAppResponse, 0, len(phones))
	for _, p := range phones {
		num := p
		if len(num) > 0 && num[0] == '+' {
			num = num[1:]
		}
		out = append(out, types.IsOnWhatsAppResponse{
			Query: p,
			JID:   types.NewJID(num, types.DefaultUserServer),
			IsIn:  true,
		})
	}
	return out, nil
}

func (f *fakeContactClient) GetContact(_ context.Context, user types.JID) (types.ContactInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.contacts[user.ToNonAD().String()]; ok {
		return c, nil
	}
	return types.ContactInfo{}, nil
}

func (f *fakeContactClient) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lidToPN[lid.ToNonAD().String()], nil
}

func (f *fakeContactClient) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pnToLID[pn.ToNonAD().String()], nil
}

func (f *fakeContactClient) GetUserInfo(_ context.Context, jids []types.JID) (map[types.JID]types.UserInfo, error) {
	f.userInfoHits.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[types.JID]types.UserInfo, len(jids))
	for _, j := range jids {
		if info, ok := f.userInfo[j]; ok {
			out[j] = info
		}
	}
	return out, nil
}

func (f *fakeContactClient) GetProfilePictureInfo(_ context.Context, jid types.JID, _ *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
	f.picHits.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	key := jid.ToNonAD().String()
	if err, ok := f.picErr[key]; ok {
		return nil, err
	}
	return f.pics[key], nil
}

// TestCheckNumbers_SingleflightMenggabung memastikan 10 request bersamaan hanya memicu 1 panggilan IsOnWhatsApp dan hasilnya ter-cache di LRU.
func TestCheckNumbers_SingleflightMenggabung(t *testing.T) {
	gate := make(chan struct{})
	fc := &fakeContactClient{gate: gate}
	inst := &Instance{
		id:         "sales",
		checkLRU:   NewLRU[string, NumberCheckResult](2048, 24*time.Hour),
		contactLRU: NewLRU[string, ContactProfile](256, time.Hour),
		contactCli: fc,
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Urutan dibalik separuh request untuk memastikan pengurutan batch menyatukan key singleflight.
			nums := []string{"628111111111", "628222222222"}
			if idx%2 == 1 {
				nums = []string{"628222222222", "628111111111"}
			}
			res, err := inst.CheckNumbers(context.Background(), nums)
			if err != nil {
				errs <- err
				return
			}
			if len(res) != 2 || !res[0].Exists || !res[1].Exists {
				errs <- errors.New("hasil CheckNumbers tidak sesuai")
			}
		}(n)
	}

	// Lepas barrier supaya seluruh goroutine sudah menunggu singleflight atau memproses serempak.
	time.Sleep(25 * time.Millisecond)
	close(gate)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("CheckNumbers concurrent gagal: %v", err)
	}
	if hits := fc.isOnWAHits.Load(); hits != 1 {
		t.Fatalf("diharapkan tepat 1 panggilan IsOnWhatsApp untuk 10 request identik bersamaan, didapat %d", hits)
	}

	// Panggilan berikutnya harus dilayani penuh dari LRU cache tanpa memanggil server lagi.
	res2, err := inst.CheckNumbers(context.Background(), []string{"628222222222"})
	if err != nil || len(res2) != 1 || !res2[0].Exists {
		t.Fatalf("cache hit CheckNumbers gagal: res=%+v err=%v", res2, err)
	}
	if hits := fc.isOnWAHits.Load(); hits != 1 {
		t.Fatalf("cache hit tidak boleh menambah panggilan IsOnWhatsApp, didapat %d", hits)
	}
}

// TestContactProfile_CacheDanLID memverifikasi resolusi LID->PN sebelum GetUserInfo, penanganan foto tersembunyi, dan cache LRU 1 jam.
func TestContactProfile_CacheDanLID(t *testing.T) {
	lid := types.NewJID("123456789012", types.HiddenUserServer)
	pn := types.NewJID("6281234567890", types.DefaultUserServer)

	fc := &fakeContactClient{
		lidToPN: map[string]types.JID{lid.String(): pn},
		contacts: map[string]types.ContactInfo{
			pn.String(): {
				Found:        true,
				PushName:     "Sari",
				FullName:     "Sari Wijaya",
				BusinessName: "Toko Sari",
			},
		},
		userInfo: map[types.JID]types.UserInfo{
			pn: {
				Status: "Sedang sibuk",
				LID:    lid,
			},
		},
		picErr: map[string]error{
			pn.String(): whatsmeow.ErrProfilePictureNotSet,
		},
	}

	inst := &Instance{
		id:         "sales",
		checkLRU:   NewLRU[string, NumberCheckResult](2048, 24*time.Hour),
		contactLRU: NewLRU[string, ContactProfile](256, time.Hour),
		contactCli: fc,
	}

	prof, err := inst.GetContactProfile(context.Background(), lid.String())
	if err != nil {
		t.Fatalf("GetContactProfile gagal: %v", err)
	}
	if prof.JID != lid.String() || prof.PN != pn.String() || prof.LID != lid.String() {
		t.Fatalf("identitas kontak tidak sesuai: %+v", prof)
	}
	if prof.PushName != "Sari" || prof.FullName != "Sari Wijaya" || prof.BusinessName != "Toko Sari" || prof.About != "Sedang sibuk" {
		t.Fatalf("profil kontak tidak sesuai: %+v", prof)
	}
	if prof.Picture != nil || prof.VerifiedName != "" {
		t.Fatalf("picture harus nil saat tidak disetel & verified_name kosong, didapat: %+v", prof)
	}
	if fc.userInfoHits.Load() != 1 || fc.picHits.Load() != 1 {
		t.Fatalf("diharapkan 1 panggilan GetUserInfo & 1 GetProfilePictureInfo, didapat ui=%d pic=%d", fc.userInfoHits.Load(), fc.picHits.Load())
	}

	// Panggilan kedua untuk JID yang sama harus memakai cache LRU tanpa menyentuh server.
	prof2, err := inst.GetContactProfile(context.Background(), lid.String())
	if err != nil || prof2.PN != pn.String() {
		t.Fatalf("panggilan kedua dari cache gagal: %+v err=%v", prof2, err)
	}
	if fc.userInfoHits.Load() != 1 || fc.picHits.Load() != 1 {
		t.Fatalf("panggilan kedua seharusnya diambil dari cache, didapat ui=%d pic=%d", fc.userInfoHits.Load(), fc.picHits.Load())
	}

	// Kasus foto Unauthorized (401) + VerifiedName + foto ada lewat PN.
	pn2 := types.NewJID("6289876543210", types.DefaultUserServer)
	fc.picErr[pn2.String()] = whatsmeow.ErrProfilePictureUnauthorized
	prof3, err := inst.GetContactProfile(context.Background(), pn2.String())
	if err != nil || prof3.Picture != nil {
		t.Fatalf("foto unauthorized harus tetap 200 dengan picture nil, didapat prof=%+v err=%v", prof3, err)
	}
}
