package cli

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"context"

	"github.com/fikfikk-org/waapi/internal/control"
)

func runDebug(ctx context.Context, ctrl *control.Client, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("penggunaan: waapi debug pprof <heap|goroutine|goroutineleak|profile> [-o file] [--seconds N]")
	}
	if args[0] != "pprof" {
		return fmt.Errorf("subperintah debug tidak dikenal: %q (gunakan: pprof)", args[0])
	}
	return runDebugPprof(ctx, ctrl, args[1:])
}

func runDebugPprof(ctx context.Context, ctrl *control.Client, args []string) error {
	var profName, outFile string
	seconds := 0

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-o" || a == "--output":
			if i+1 >= len(args) {
				return errors.New("flag -o membutuhkan path file keluaran")
			}
			i++
			outFile = args[i]
		case strings.HasPrefix(a, "-o="):
			outFile = strings.TrimPrefix(a, "-o=")
		case strings.HasPrefix(a, "--output="):
			outFile = strings.TrimPrefix(a, "--output=")
		case a == "--seconds" || a == "-seconds":
			if i+1 >= len(args) {
				return errors.New("flag --seconds membutuhkan nilai angka")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return fmt.Errorf("nilai --seconds harus bilangan bulat positif: %q", args[i])
			}
			seconds = n
		case strings.HasPrefix(a, "--seconds="):
			raw := strings.TrimPrefix(a, "--seconds=")
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				return fmt.Errorf("nilai --seconds harus bilangan bulat positif: %q", raw)
			}
			seconds = n
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("opsi debug pprof tidak dikenal: %q", a)
		default:
			if profName != "" {
				return fmt.Errorf("argumen berlebih: %q", a)
			}
			profName = a
		}
	}

	switch profName {
	case "heap", "goroutine", "goroutineleak", "profile":
	case "":
		return errors.New("nama profil wajib diisi: heap | goroutine | goroutineleak | profile")
	default:
		return fmt.Errorf("profil %q tidak didukung (pilih: heap | goroutine | goroutineleak | profile)", profName)
	}

	path := "/control/debug/pprof/" + profName
	if seconds > 0 {
		path += "?seconds=" + strconv.Itoa(seconds)
	}

	timeout := 15 * time.Second
	if profName == "profile" {
		durSec := seconds
		if durSec <= 0 {
			durSec = 30
		}
		timeout = time.Duration(durSec)*time.Second + 15*time.Second
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if outFile == "" {
		return ctrl.FetchToWriter(reqCtx, path, os.Stdout)
	}

	f, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("buka file keluaran %s: %w", outFile, err)
	}
	defer f.Close()

	if err := ctrl.FetchToWriter(reqCtx, path, f); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Profil %s tersimpan di %s\n", profName, outFile)
	return nil
}
