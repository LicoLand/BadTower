package nodepackage

import (
	"os"
)

// UpgradeRollbackPreflight guards an upgrade or rollback of one native store
// before any destructive step runs (REQ-018). It fails when the current and
// candidate store identities do not match the native identity, and refuses
// when any retired state path exists on disk, returning every discovered
// retired path as a finding.
func UpgradeRollbackPreflight(currentStoreIdentity, candidateStoreFormat string, retiredPaths []string) ([]string, error) {
	if currentStoreIdentity != StationStoreFormat || candidateStoreFormat != StationStoreFormat {
		return nil, NewFailure(CodePreflightIdentityMismatch)
	}
	if currentStoreIdentity != candidateStoreFormat {
		return nil, NewFailure(CodePreflightIdentityMismatch)
	}
	var found []string
	for _, path := range retiredPaths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			found = append(found, path)
		}
	}
	if len(found) > 0 {
		return found, NewFailure(CodePreflightRetiredFound)
	}
	return nil, nil
}
