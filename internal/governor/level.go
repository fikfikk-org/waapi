// Package governor menyediakan sinyal level tekanan memori untuk degradasi bertahap.
package governor

// Level merepresentasikan status tekanan memori daemon saat ini.
type Level int32

const (
	// Green menandakan konsumsi memori berada dalam batas aman.
	Green Level = iota
	// Yellow menandakan tekanan memori meningkat dan perlu pembatasan ringan.
	Yellow
	// Red menandakan kondisi kritis sehingga request baru harus ditolak.
	Red
)

// String mengembalikan representasi teks level tekanan memori.
func (l Level) String() string {
	switch l {
	case Green:
		return "green"
	case Yellow:
		return "yellow"
	case Red:
		return "red"
	default:
		return "unknown"
	}
}

// Source adalah antarmuka pembaca level tekanan memori terkini.
type Source interface {
	Level() Level
}

type alwaysGreenSource struct{}

// Level selalu mengembalikan Green pada stub pra-governor.
func (alwaysGreenSource) Level() Level { return Green }

// AlwaysGreen menyediakan Source default sebelum implementasi governor aktif di Batch 10.
var AlwaysGreen Source = alwaysGreenSource{}
