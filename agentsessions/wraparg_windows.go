//go:build windows

package agentsessions

import (
	"errors"
	"os/exec"
)

// applyResourceLimits is a no-op on Windows when limits is nil/zero.
// Non-zero limits return an error since the sh -c ulimit wrap is unix-only.
// The PTY runtime is already gated to non-windows builds, so this only
// covers the rare case of someone wiring ResourceLimits onto a windows-only
// future runtime.
func applyResourceLimits(cmd *exec.Cmd, limits *ResourceLimits) (func(), error) {
	if limits == nil || limits.IsZero() {
		return func() {}, nil
	}
	return nil, errors.New("agentsessions: ResourceLimits unsupported on windows")
}
