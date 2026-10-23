package jetq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Default policy values used when neither the submission, the task nor the
// producer sets a field.
const (
	DefaultMaxAttempts = 3
)

// defaultBackoff returns the rule used when no layer sets one.
func defaultBackoff() BackoffSpec { return ExponentialBackoff(5*time.Second, 10*time.Minute) }

// Policy is a partial execution policy. Unset fields inherit from the next
// layer: a submission override, then the [Task] default, then the producer
// default. A nil Timeout inherits; a Timeout pointing at zero means no limit.
type Policy struct {
	// MaxAttempts is the business execution budget; 0 inherits.
	MaxAttempts int
	// Timeout bounds one execution; nil inherits, zero means no limit.
	Timeout *time.Duration
	// Backoff decides the delay before a retry; nil inherits.
	Backoff *BackoffSpec
}

// NoTimeout returns a timeout pointer meaning "no limit", for [Policy.Timeout].
func NoTimeout() *time.Duration { d := time.Duration(0); return &d }

// TimeoutOf returns a timeout pointer for [Policy.Timeout].
func TimeoutOf(d time.Duration) *time.Duration { return &d }

// Validate reports whether a partial policy layer is well formed. Unset fields
// are fine; negative or malformed values are errors.
func (p Policy) Validate() error { return p.validate() }

// validate rejects values that are invalid at any layer.
func (p Policy) validate() error {
	if p.MaxAttempts < 0 {
		return fmt.Errorf("jetq: max attempts must not be negative, got %d", p.MaxAttempts)
	}
	if p.Timeout != nil && *p.Timeout < 0 {
		return fmt.Errorf("jetq: timeout must not be negative, got %s", *p.Timeout)
	}
	if p.Backoff != nil {
		if err := p.Backoff.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// ExecutionPolicy is a fully resolved policy. It is fixed at submission and
// stored in the [Envelope]; workers never override it.
type ExecutionPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Timeout     time.Duration `json:"timeout"` // zero: no limit
	Backoff     BackoffSpec   `json:"backoff"`
}

// Validate reports whether a resolved policy can be executed.
func (p ExecutionPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return fmt.Errorf("jetq: max attempts must be at least 1, got %d", p.MaxAttempts)
	}
	if p.Timeout < 0 {
		return fmt.Errorf("jetq: timeout must not be negative, got %s", p.Timeout)
	}
	return p.Backoff.Validate()
}

// ResolvePolicy merges the layers in priority order: submission, task,
// producer, then the package defaults. Each layer is validated, so an invalid
// value is reported even when a higher layer overrides it.
func ResolvePolicy(submission, task, producer Policy) (ExecutionPolicy, error) {
	for _, layer := range []Policy{submission, task, producer} {
		if err := layer.validate(); err != nil {
			return ExecutionPolicy{}, err
		}
	}
	resolved := ExecutionPolicy{MaxAttempts: DefaultMaxAttempts, Backoff: defaultBackoff()}
	// Apply from lowest to highest priority.
	for _, layer := range []Policy{producer, task, submission} {
		if layer.MaxAttempts > 0 {
			resolved.MaxAttempts = layer.MaxAttempts
		}
		if layer.Timeout != nil {
			resolved.Timeout = *layer.Timeout
		}
		if layer.Backoff != nil {
			resolved.Backoff = *layer.Backoff
		}
	}
	return resolved, resolved.Validate()
}

// BackoffKind names a serializable backoff rule.
type BackoffKind string

// Backoff kinds.
const (
	BackoffConstant    BackoffKind = "constant"
	BackoffExponential BackoffKind = "exponential"
)

// BackoffSpec is a serializable retry delay rule. It travels in the
// [Envelope], so every worker computes the same delays.
type BackoffSpec struct {
	Kind BackoffKind `json:"kind"`
	// Base is the constant delay, or the first exponential delay.
	Base time.Duration `json:"base"`
	// Limit caps exponential delays; unused for constant.
	Limit time.Duration `json:"limit,omitempty"`
}

// ConstantBackoff waits delay after every failed execution.
func ConstantBackoff(delay time.Duration) BackoffSpec {
	return BackoffSpec{Kind: BackoffConstant, Base: delay}
}

// ExponentialBackoff doubles the delay after every failed execution, starting
// at base and capped at limit.
func ExponentialBackoff(base, limit time.Duration) BackoffSpec {
	return BackoffSpec{Kind: BackoffExponential, Base: base, Limit: limit}
}

// Validate reports whether the spec is well formed. Unknown kinds are errors:
// a worker never falls back to another rule.
func (b BackoffSpec) Validate() error {
	switch b.Kind {
	case BackoffConstant:
		if b.Base < 0 {
			return fmt.Errorf("jetq: constant backoff delay must not be negative, got %s", b.Base)
		}
		if b.Limit != 0 {
			return errors.New("jetq: constant backoff has no limit")
		}
	case BackoffExponential:
		if b.Base <= 0 {
			return fmt.Errorf("jetq: exponential backoff base must be positive, got %s", b.Base)
		}
		if b.Limit < b.Base {
			return fmt.Errorf("jetq: exponential backoff limit %s is below base %s", b.Limit, b.Base)
		}
	case "":
		return errors.New("jetq: backoff kind is empty")
	default:
		return fmt.Errorf("jetq: unknown backoff kind %q", b.Kind)
	}
	return nil
}

// Delay returns the delay before the execution following the given failed
// one (1-based).
func (b BackoffSpec) Delay(attempt int) (time.Duration, error) {
	if err := b.Validate(); err != nil {
		return 0, err
	}
	if attempt < 1 {
		return 0, fmt.Errorf("jetq: attempt must be at least 1, got %d", attempt)
	}
	if b.Kind == BackoffConstant {
		return b.Base, nil
	}
	delay := b.Base
	for i := 1; i < attempt && delay < b.Limit; i++ {
		if delay > b.Limit/2 {
			delay = b.Limit
		} else {
			delay *= 2
		}
	}
	return delay, nil
}

// UnmarshalJSON decodes and validates a spec, so a stored envelope with an
// unknown rule fails to decode instead of silently using another rule.
func (b *BackoffSpec) UnmarshalJSON(data []byte) error {
	type plain BackoffSpec
	var tmp plain
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if err := BackoffSpec(tmp).Validate(); err != nil {
		return err
	}
	*b = BackoffSpec(tmp)
	return nil
}
