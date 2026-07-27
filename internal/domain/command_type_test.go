package domain

import "testing"

func TestCommandTypeIsClosed(t *testing.T) {
	for _, commandType := range []CommandType{CommandPrepare, CommandStart, CommandCancel} {
		if err := commandType.Validate("command.type"); err != nil {
			t.Fatalf("Validate(%q): %v", commandType, err)
		}
	}
	if err := CommandType("shell").Validate("command.type"); err == nil {
		t.Fatal("unknown command type was accepted")
	}
}
