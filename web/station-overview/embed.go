// Package stationoverview embeds the verified production credential-gate
// shell and its two content-addressed assets. The allowlist is deliberately
// exact: adding a file below dist does not make it remotely addressable.
package stationoverview

import (
	"embed"
	"io/fs"
	"regexp"
	"strings"
)

const IndexFile = "index.html"

// The extension-specific globs make Vite content hashes reproducible inputs
// without embedding source maps, metadata, or arbitrary file types. The
// allowlist below exposes only the two hashed files referenced by index.html,
// even if stale JS or CSS happens to remain in dist/assets.
//
//go:embed dist/index.html dist/assets/*.js dist/assets/*.css
var dist embed.FS

// buildRoot is the embedded directory holding the build output itself.
const buildRoot = "dist"

var (
	hashedAssetReference = regexp.MustCompile(`\./(assets/[A-Za-z0-9][A-Za-z0-9._-]{0,63}-[A-Za-z0-9_-]{8,64}\.(?:js|css))["']`)
	buildAssets          = mustBuildAssets()
)

// AssetFiles returns the exact script and stylesheet selected by the embedded
// index. The fixed-size value is a copy and cannot widen the route allowlist.
func AssetFiles() [2]string {
	return buildAssets
}

func mustBuildAssets() [2]string {
	index, err := fs.ReadFile(dist, buildRoot+"/"+IndexFile)
	if err != nil {
		panic("station overview index is unavailable")
	}
	matches := hashedAssetReference.FindAllSubmatch(index, -1)
	if len(matches) != 2 {
		panic("station overview index must reference one hashed script and one hashed stylesheet")
	}
	var assets [2]string
	for _, match := range matches {
		name := string(match[1])
		position := -1
		switch {
		case strings.HasSuffix(name, ".js"):
			position = 0
		case strings.HasSuffix(name, ".css"):
			position = 1
		}
		if position < 0 || assets[position] != "" {
			panic("station overview index has an invalid asset set")
		}
		if _, err := fs.Stat(dist, buildRoot+"/"+name); err != nil {
			panic("station overview referenced asset is unavailable")
		}
		assets[position] = name
	}
	if assets[0] == "" || assets[1] == "" {
		panic("station overview index has an incomplete asset set")
	}
	return assets
}

// File returns one allowlisted embedded artifact. All other names fail before
// any filesystem lookup, including traversal, directory, source-map, and
// future unreviewed asset names.
func File(name string) ([]byte, bool) {
	assets := AssetFiles()
	if name != IndexFile && name != assets[0] && name != assets[1] {
		return nil, false
	}
	content, err := fs.ReadFile(dist, buildRoot+"/"+name)
	if err != nil {
		return nil, false
	}
	return content, true
}
