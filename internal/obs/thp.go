package obs

import (
	"os"
	"strings"
)

const defaultTHPPath = "/sys/kernel/mm/transparent_hugepage/enabled"

// ReadTHPMode membaca mode Transparent Huge Pages aktif (kata di dalam kurung siku [...]).
func ReadTHPMode(path string) string {
	if strings.TrimSpace(path) == "" {
		path = defaultTHPPath
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, tok := range strings.Fields(string(b)) {
		if strings.HasPrefix(tok, "[") && strings.HasSuffix(tok, "]") && len(tok) > 2 {
			return tok[1 : len(tok)-1]
		}
	}
	return ""
}
