package agentsessions

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// defaultIDFn returns a 32-char hex string from crypto/rand. Used by the
// Manager to mint attach ids when the consumer doesn't override via
// WithIDFn. Stays in stdlib to keep the lib's go.mod minimal.
func defaultIDFn() string {
	defaultIDMu.Lock()
	defer defaultIDMu.Unlock()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand can't fail on a healthy system; if it does the
		// caller's session-id namespace will collide, which is the
		// failure they'd want to know about — return a sentinel.
		return "agentsessions-rand-failure"
	}
	return hex.EncodeToString(b[:])
}

var defaultIDMu sync.Mutex
