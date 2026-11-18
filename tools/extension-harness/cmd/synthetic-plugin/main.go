package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
)

func main() {
	descriptorPath := flag.String("descriptor", "", "closed synthetic descriptor")
	flag.Parse()
	file, err := os.Open(*descriptorPath)
	if err != nil {
		fail()
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		fail()
	}
	if _, err := extension.DecodeDescriptor(raw); err != nil {
		fail()
	}
	_, _ = fmt.Fprintln(os.Stdout, "SYNTHETIC_PLUGIN_DESCRIPTOR_OK")
}

func fail() {
	_, _ = fmt.Fprintln(os.Stderr, "SYNTHETIC_PLUGIN_INVALID")
	os.Exit(1)
}
