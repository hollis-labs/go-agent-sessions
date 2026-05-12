package agentsessions

import (
	"bytes"
	"errors"
	"log"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestLogAbnormalWait(t *testing.T) {
	// Capture log.Default() output for the duration of this test. Done in a
	// single test function (not table-driven across t.Run subtests) because
	// log output is process-global state and parallel subtests would race
	// on the buffer.
	origOut := log.Writer()
	origFlags := log.Flags()
	defer func() {
		log.SetOutput(origOut)
		log.SetFlags(origFlags)
	}()
	log.SetFlags(0) // drop timestamp prefix for stable assertions

	cleanExit := func() error { return nil }()
	// Build a real *exec.ExitError via a process that returns non-zero.
	// Run() calls Wait() internally and returns the *exec.ExitError when
	// the process exits with a non-zero code.
	exitErrErr := exec.Command("/bin/sh", "-c", "exit 7").Run()
	var asExitErr *exec.ExitError
	if !errors.As(exitErrErr, &asExitErr) {
		t.Skipf("unable to produce *exec.ExitError fixture (got %T: %v)", exitErrErr, exitErrErr)
	}

	syscallErr := errors.New("wait: no child processes")

	cases := []struct {
		name      string
		pid       int
		elapsed   time.Duration
		err       error
		wantLog   bool
		wantField string
	}{
		{
			name:    "clean exit, healthy duration → no log",
			pid:     1234,
			elapsed: 5 * time.Second,
			err:     cleanExit,
			wantLog: false,
		},
		{
			name:    "exit-error, healthy duration → no log",
			pid:     1234,
			elapsed: 5 * time.Second,
			err:     exitErrErr,
			wantLog: false,
		},
		{
			name:      "non-exit-error, healthy duration → log",
			pid:       1234,
			elapsed:   5 * time.Second,
			err:       syscallErr,
			wantLog:   true,
			wantField: "wait: no child processes",
		},
		{
			name:      "clean exit, rapid duration → log",
			pid:       1234,
			elapsed:   10 * time.Millisecond,
			err:       cleanExit,
			wantLog:   true,
			wantField: "elapsed=10ms",
		},
		{
			name:      "non-exit-error, rapid duration → log (single line)",
			pid:       4321,
			elapsed:   11 * time.Millisecond,
			err:       syscallErr,
			wantLog:   true,
			wantField: "session=test-rt pid=4321",
		},
		{
			name:    "zero elapsed (spawnedAt unset) → not treated as rapid",
			pid:     1234,
			elapsed: 0,
			err:     cleanExit,
			wantLog: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log.SetOutput(&buf)

			logAbnormalWait("streaming-stdio", "test-rt", tc.pid, tc.elapsed, tc.err)

			got := buf.String()
			if tc.wantLog {
				if got == "" {
					t.Errorf("expected log line, got nothing")
				}
				if !strings.HasPrefix(got, "agentsessions: streaming-stdio waiter abnormal:") {
					t.Errorf("log line missing expected prefix; got: %q", got)
				}
				if tc.wantField != "" && !strings.Contains(got, tc.wantField) {
					t.Errorf("log line missing %q; got: %q", tc.wantField, got)
				}
			} else if got != "" {
				t.Errorf("expected no log line, got: %q", got)
			}
		})
	}
}
