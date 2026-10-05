package manifest

import _ "embed"

// ManifestJSON is the module's muxcore.json. Its "version" field is the
// single source of the reported module version (ADR-0021).
//
//go:embed muxcore.json
var ManifestJSON []byte
