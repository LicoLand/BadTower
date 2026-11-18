package architecture

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCoreFirewallImportGraph(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	command := exec.Command("go", "list", "-deps", "./cmd/badtower")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, forbidden := range []string{
		"/plugins/", "/pkg/extension/", "/internal/pluginpackage", "licoarc-candidate",
	} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("native Station dependency graph contains %q", forbidden)
		}
	}
}

func TestPackageBoundaryBuildsSeparateProcesses(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	for _, target := range []string{"./cmd/badtower", "./plugins/licoarc-candidate/cmd/badtower-licoarc-candidate"} {
		command := exec.Command("go", "list", "-f", "{{.Name}}", target)
		command.Dir = root
		output, err := command.Output()
		if err != nil {
			t.Fatalf("go list %s: %v", target, err)
		}
		if strings.TrimSpace(string(output)) != "main" {
			t.Fatalf("%s is not a separate process package", target)
		}
	}
}

func TestNativeExcellenceSourceBoundary(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	for _, relative := range []string{
		"internal/stationcore/machine.go", "internal/stationcluster/member_runtime.go",
		"internal/stationcluster/raftlog.go", "internal/stationcluster/peer_transport.go",
		"contracts/extension/v1/plugin-descriptor.schema.json", "tools/extension-harness/harness.go",
	} {
		if info, err := os.Stat(filepath.Join(root, relative)); err != nil || info.IsDir() {
			t.Fatalf("required source boundary missing: %s", relative)
		}
	}
	for _, relative := range []string{
		"internal/httpapi", "internal/profile", "internal/station", "internal/plugin", "internal/plugins",
	} {
		if _, err := os.Stat(filepath.Join(root, relative)); !os.IsNotExist(err) {
			t.Fatalf("retired or in-process boundary exists: %s", relative)
		}
	}
}

func TestRuntimeDependencyAbsence(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	command := exec.Command("rg", "-n", "LicoArc/|licoarc\\.bundle|conformance/v1/transport", "plugins/licoarc-candidate", "--glob", "!**/*_test.go", "--glob", "!**/testdata/**")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("production adapter contains runtime protocol source dependency: %s", strings.TrimSpace(string(output)))
	}
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("runtime-dependency scan failed: %v", err)
	}
}

func TestStatusDimensions(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docs", "STATUS.md"))
	if err != nil {
		t.Fatal(err)
	}
	status := string(raw)
	for _, heading := range []string{"## Intent", "## Implementation", "## Verification", "## Release", "## Support", "## Operation"} {
		if !strings.Contains(status, heading) {
			t.Fatalf("status dimension missing: %s", heading)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
