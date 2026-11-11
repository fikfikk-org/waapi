// Package cli mengimplementasikan seluruh subcommand terminal waapi tanpa membuka database secara langsung.
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mdp/qrterminal/v3"
)

// newTabWriter membuat tabwriter standar untuk output tabel CLI yang rapi.
func newTabWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
}

// renderQR mencetak kode QR ASCII langsung di terminal.
func renderQR(code string) {
	qrterminal.GenerateHalfBlock(code, qrterminal.L, os.Stdout)
}

// formatBytes mengubah ukuran byte menjadi string KB/MB yang mudah dibaca.
func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
	)
	switch {
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%d KB", b/kb)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatTime mengubah unix timestamp menjadi tanggal lokal atau "-" jika nol.
func formatTime(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.Unix(ts, 0).Format("2006-01-02 15:04:05")
}

// formatDuration mengubah detik uptime menjadi format ringkas seperti 3d4h12m.
func formatDuration(sec int64) string {
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}
	days := sec / 86400
	sec %= 86400
	hours := sec / 3600
	sec %= 3600
	mins := sec / 60
	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if mins > 0 && days == 0 {
		parts = append(parts, fmt.Sprintf("%dm", mins))
	}
	return strings.Join(parts, "")
}

