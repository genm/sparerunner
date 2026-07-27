package domain

import "strings"

// MaxTargetIDBytes bounds an owner-supplied Target identifier before it can
// reach SQLite or the agent-controller wire. It is not a product quota on how
// many Targets exist: it is the storage boundary for one identifier, sized well
// above every identifier this system generates, so a hostile desktop client
// cannot push unbounded text through the local control endpoint.
const MaxTargetIDBytes = 128

// ValidateShape checks only what every surface must agree on for a Target
// identifier to be storable and transportable: non-empty, not surrounded by
// whitespace, bounded, and free of control characters. It deliberately does not
// check existence. The node owner may exclude a Target this node has never been
// told about — while offline, or before the first eligible-target list arrives —
// and that is a safe no-op rendered as not-currently-eligible, never an error.
func (id TargetID) ValidateShape(field string) error {
	value := string(id)
	if value == "" {
		return invalid("required", field, "must not be empty")
	}
	if strings.TrimSpace(value) != value {
		return invalid("invalid_target_id", field, "must not be surrounded by whitespace")
	}
	if len(value) > MaxTargetIDBytes {
		return invalid("invalid_target_id", field, "is longer than the identifier boundary")
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return invalid("invalid_target_id", field, "must not contain control characters")
		}
	}
	return nil
}
