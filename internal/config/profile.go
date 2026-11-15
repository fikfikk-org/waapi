package config

import (
	"fmt"
	"strings"
)

// Profile menentukan preset batas memori dan ukuran cache SQLite untuk daemon.
type Profile string

const (
	ProfileMicro   Profile = "micro"
	ProfileLean    Profile = "lean"
	ProfileClassic Profile = "classic"
)

// ProfileValues menyimpan seluruh parameter turunan dari sebuah profil runtime.
type ProfileValues struct {
	MemoryLimitMiB  int
	AppCacheKiB     int
	JournalCacheKiB int
	WACacheKiB      int
	WebhookLanes    int
	SSEMax          int
	MemoryBudgetMiB int
}

// ParseProfile memvalidasi string nama profil dan mengembalikan nilai Profile yang sesuai.
func ParseProfile(raw string) (Profile, error) {
	switch Profile(strings.ToLower(strings.TrimSpace(raw))) {
	case ProfileMicro:
		return ProfileMicro, nil
	case ProfileLean, "":
		return ProfileLean, nil
	case ProfileClassic:
		return ProfileClassic, nil
	default:
		return "", fmt.Errorf("WAAPI_PROFILE harus micro, lean, atau classic, didapat %q", raw)
	}
}

// ProfileDefaults mengembalikan nilai default untuk profil tertentu (fallback ke lean jika tidak dikenal).
func ProfileDefaults(p Profile) ProfileValues {
	switch p {
	case ProfileMicro:
		return ProfileValues{
			MemoryLimitMiB:  20,
			AppCacheKiB:     256,
			JournalCacheKiB: 512,
			WACacheKiB:      1024,
			WebhookLanes:    2,
			SSEMax:          4,
			MemoryBudgetMiB: 16,
		}
	case ProfileClassic:
		return ProfileValues{
			MemoryLimitMiB:  80,
			AppCacheKiB:     2000,
			JournalCacheKiB: 2000,
			WACacheKiB:      2000,
			WebhookLanes:    8,
			SSEMax:          32,
			MemoryBudgetMiB: 64,
		}
	default:
		return ProfileValues{
			MemoryLimitMiB:  28,
			AppCacheKiB:     512,
			JournalCacheKiB: 1024,
			WACacheKiB:      2000,
			WebhookLanes:    4,
			SSEMax:          8,
			MemoryBudgetMiB: 22,
		}
	}
}
