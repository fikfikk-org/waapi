// Command wa-api (waapi) adalah binary tunggal untuk daemon dan CLI WhatsApp API Gateway.
package main

import (
	"fmt"
	"os"

	"github.com/fikfikk-org/waapi/internal/cli"
)

func main() {
	if err := cli.Execute(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

