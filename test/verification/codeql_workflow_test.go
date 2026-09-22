package verification_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type codeqlWorkflowContract struct {
	On          map[string]yaml.Node `yaml:"on"`
	Concurrency struct {
		Group            string `yaml:"group"`
		CancelInProgress string `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
}

func readCodeQLWorkflowContract(t *testing.T) codeqlWorkflowContract {
	t.Helper()

	path := filepath.Join(repositoryRoot(t), ".github", "workflows", "codeql.yml")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read CodeQL workflow: %v", err)
	}
	var workflow codeqlWorkflowContract
	if err := yaml.Unmarshal(source, &workflow); err != nil {
		t.Fatalf("decode CodeQL workflow: %v", err)
	}
	return workflow
}

// A docs-only directory filter also skips executable examples. Require the
// intended prose-only exception explicitly instead of trusting the directory.
func TestCodeQLChangeTriggersExcludeOnlyMarkdown(t *testing.T) {
	workflow := readCodeQLWorkflowContract(t)

	for _, eventName := range []string{"pull_request", "push"} {
		t.Run(eventName, func(t *testing.T) {
			node, exists := workflow.On[eventName]
			if !exists {
				t.Fatalf("CodeQL trigger %s is missing", eventName)
			}
			var trigger struct {
				PathsIgnore []string `yaml:"paths-ignore"`
			}
			if err := node.Decode(&trigger); err != nil {
				t.Fatalf("decode %s trigger: %v", eventName, err)
			}
			if len(trigger.PathsIgnore) != 1 || trigger.PathsIgnore[0] != "**/*.md" {
				t.Errorf("CodeQL must exclude only Markdown, got %v", trigger.PathsIgnore)
			}
		})
	}
}

// Pending runs in one concurrency group can replace each other even when
// cancellation of an already running job is disabled. Full scans need their
// own event-scoped groups, and only obsolete PR analysis may be cancelled.
func TestCodeQLFullScansStaySeparateFromPullRequestsAndPushes(t *testing.T) {
	workflow := readCodeQLWorkflowContract(t)

	for _, eventName := range []string{"schedule", "workflow_dispatch"} {
		if _, exists := workflow.On[eventName]; !exists {
			t.Errorf("CodeQL full-scan trigger %s is missing", eventName)
		}
	}
	if !strings.Contains(workflow.Concurrency.Group, "${{ github.event_name }}") {
		t.Error("CodeQL concurrency must separate push, schedule, and manual events")
	}
	if workflow.Concurrency.CancelInProgress != "${{ github.event_name == 'pull_request' }}" {
		t.Error("only superseded pull-request CodeQL scans may be cancelled")
	}
}
