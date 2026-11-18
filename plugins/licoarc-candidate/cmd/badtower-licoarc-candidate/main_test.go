package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigRequiresExactSchemaAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	raw := fmt.Sprintf(`{"schema":%q,"listener":{"address":"127.0.0.1:443","certificatePath":"cert.pem","privateKeyPath":"key.pem"},"native":{"endpoint":"station.test:443","serverName":"station.test","caPath":"ca.pem","namespace":"ns_AAAAAAAAAAAAAAAAAAAAAA","bearer":"secret"},"stateRoot":"/state","signingKeyPath":"signer.pem","stationDescriptorDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","descriptorNotAfter":%d,"budgets":{"maxConcurrentOperations":64,"maxQueuedOperations":256,"maxStateBytes":1073741824}}`,
		configSchema, time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err != nil {
		t.Fatal(err)
	}
	upperDigest := strings.Replace(raw, strings.Repeat("a", 64), strings.Repeat("A", 64), 1)
	if err := os.WriteFile(path, []byte(upperDigest), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("non-canonical Station descriptor digest accepted")
	}
	duplicateDigest := strings.Replace(raw,
		`"stationDescriptorDigest":"`+strings.Repeat("a", 64)+`"`,
		`"stationDescriptorDigest":"`+strings.Repeat("a", 64)+`","stationDescriptorDigest":"`+strings.Repeat("a", 64)+`"`, 1)
	if err := os.WriteFile(path, []byte(duplicateDigest), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("duplicate Station descriptor digest accepted")
	}
	if err := os.WriteFile(path, []byte(raw[:len(raw)-1]+`,"binding":"runtime"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("runtime binding override accepted")
	}
}
