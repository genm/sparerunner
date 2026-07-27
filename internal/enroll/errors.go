package enroll

import "errors"

// EnrollmentFailure is the transport-safe outcome of an enrollment attempt.
// It deliberately carries no request credential or storage error detail.
type EnrollmentFailure uint8

const (
	EnrollmentFailureUnknown EnrollmentFailure = iota
	EnrollmentFailureMalformed
	EnrollmentFailureRejected
	EnrollmentFailureUnavailable
)

var (
	ErrEnrollmentMalformed   = errors.New("malformed enrollment request")
	ErrEnrollmentRejected    = errors.New("enrollment credential rejected")
	ErrEnrollmentUnavailable = errors.New("enrollment authority unavailable")
)

// EnrollmentFailureOf maps a Service.Enroll error to the only outcomes the
// unauthenticated HTTP boundary may expose. Unknown errors fail closed as
// unavailable rather than being misreported as client credentials.
func EnrollmentFailureOf(err error) EnrollmentFailure {
	switch {
	case err == nil:
		return EnrollmentFailureUnknown
	case errors.Is(err, ErrEnrollmentMalformed):
		return EnrollmentFailureMalformed
	case errors.Is(err, ErrEnrollmentRejected):
		return EnrollmentFailureRejected
	default:
		return EnrollmentFailureUnavailable
	}
}

func malformedEnrollment(err error) error {
	return errors.Join(ErrEnrollmentMalformed, err)
}

func rejectedEnrollment(err error) error {
	return errors.Join(ErrEnrollmentRejected, err)
}

func unavailableEnrollment(err error) error {
	return errors.Join(ErrEnrollmentUnavailable, err)
}

func registryEnrollmentFailure(err error) EnrollmentFailure {
	switch {
	case errors.Is(err, ErrTokenNotFound),
		errors.Is(err, ErrTokenEpochMismatch),
		errors.Is(err, ErrTokenSecretMismatch),
		errors.Is(err, ErrControllerFingerprintMismatch),
		errors.Is(err, ErrCredentialRejected):
		return EnrollmentFailureRejected
	default:
		return EnrollmentFailureUnavailable
	}
}
