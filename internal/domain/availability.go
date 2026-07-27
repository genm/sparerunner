package domain

// AvailabilityIntent is the node owner's local decision about whether this
// computer accepts new jobs. It is a second authority beside the Controller's
// NodeAdministrativeState, and it is deliberately monotone: effective admission
// is the conjunction of both, so a local intent can only withhold this node's
// capacity and never re-admit a node the Controller refuses.
type AvailabilityIntent string

const (
	AvailabilityAccepting AvailabilityIntent = "accepting"
	AvailabilityStopped   AvailabilityIntent = "stopped"
)

func (intent AvailabilityIntent) Validate(field string) error {
	switch intent {
	case AvailabilityAccepting, AvailabilityStopped:
		return nil
	default:
		return invalid("invalid_availability_intent", field, "must be accepting or stopped")
	}
}

// Accepts reports whether the node owner currently allows new admission. An
// unknown value is never treated as accepting.
func (intent AvailabilityIntent) Accepts() bool {
	return intent == AvailabilityAccepting
}
