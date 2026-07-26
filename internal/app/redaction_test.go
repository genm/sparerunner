package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestApplicationStateAndJoinOptionsRedactSecretMaterial(t *testing.T) {
	const canary = "secret-canary.example.test"
	var admin [32]byte
	copy(admin[:], canary)
	values := []any{
		ControllerState{Directory: "/controller", AdminSession: admin, Epoch: 4},
		AgentState{Directory: "/agent", PrivateKey: []byte(canary), NodeID: "node", Endpoint: "wss://controller.example.test"},
		JoinOptions{StateDirectory: "/agent", JoinCode: "twk_" + canary, Controller: "https://controller.example.test"},
	}
	for _, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var log bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&log, nil))
		logger.Info("value", "value", value)
		rendered := fmt.Sprintf("%v\n%+v\n%#v\n%s\n%s", value, value, value, encoded, log.String())
		if strings.Contains(rendered, canary) {
			t.Fatalf("%T leaked secret material: %s", value, rendered)
		}
	}
}
