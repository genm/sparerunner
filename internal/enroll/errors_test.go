package enroll

import (
	"errors"
	"fmt"
	"testing"
)

func TestEnrollmentFailureOfUsesExplicitClassesAndFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want EnrollmentFailure
	}{
		{name: "nil", err: nil, want: EnrollmentFailureUnknown},
		{name: "malformed", err: malformedEnrollment(ErrInvalidJoinCode), want: EnrollmentFailureMalformed},
		{name: "wrapped malformed", err: fmt.Errorf("request: %w", malformedEnrollment(ErrInvalidJoinCode)), want: EnrollmentFailureMalformed},
		{name: "rejected", err: rejectedEnrollment(ErrTokenNotFound), want: EnrollmentFailureRejected},
		{name: "explicit unavailable", err: unavailableEnrollment(errors.New("store offline")), want: EnrollmentFailureUnavailable},
		{name: "unknown fails closed", err: errors.New("unexpected authority failure"), want: EnrollmentFailureUnavailable},
		{name: "raw registry rejection is not transport classified", err: ErrTokenNotFound, want: EnrollmentFailureUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := EnrollmentFailureOf(test.err); got != test.want {
				t.Fatalf("failure = %d, want %d", got, test.want)
			}
		})
	}
}

func TestEnrollmentClassWrappersPreserveOwningCause(t *testing.T) {
	if err := malformedEnrollment(ErrJoinCodeVersion); !errors.Is(err, ErrEnrollmentMalformed) || !errors.Is(err, ErrJoinCodeVersion) {
		t.Fatalf("malformed wrapper = %v", err)
	}
	if err := rejectedEnrollment(ErrTokenSecretMismatch); !errors.Is(err, ErrEnrollmentRejected) || !errors.Is(err, ErrTokenSecretMismatch) {
		t.Fatalf("rejected wrapper = %v", err)
	}
	cause := errors.New("projection failed")
	if err := unavailableEnrollment(cause); !errors.Is(err, ErrEnrollmentUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("unavailable wrapper = %v", err)
	}
}
