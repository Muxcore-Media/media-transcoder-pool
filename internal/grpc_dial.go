package internal

import "github.com/Muxcore-Media/core/sdk/go/module/meshtls"

// meshInsecureAllowed reports whether the dev plaintext flag is set (ADR-0016/0017).
func meshInsecureAllowed() bool { return meshtls.Insecure() }
