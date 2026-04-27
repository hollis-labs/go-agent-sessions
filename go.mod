module github.com/hollis-labs/go-agent-sessions

go 1.26.1

require (
	github.com/hollis-labs/go-providers v0.5.0
	github.com/hollis-labs/go-runner v0.2.0
	github.com/hollis-labs/go-sandbox v0.1.0
)

// Local-dev replace until go-runner v0.2.0 publishes its tag.
// Remove this block when CW-20260427-0044's tag is cut + pushed.
replace github.com/hollis-labs/go-runner => ../go-runner

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/creack/pty v1.1.24 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
