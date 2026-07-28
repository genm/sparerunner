package sparerunner_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"testing"

	"go.yaml.in/yaml/v3"
)

var taskIDPattern = regexp.MustCompile(`^spr-[0-9]{3}$`)

type taskManifest struct {
	Version int        `yaml:"version"`
	Feature string     `yaml:"feature"`
	Tasks   []taskSpec `yaml:"tasks"`
}

type taskSpec struct {
	ID          string    `yaml:"id"`
	Title       string    `yaml:"title"`
	Type        string    `yaml:"type"`
	Description string    `yaml:"description"`
	DependsOn   []string  `yaml:"depends_on"`
	Paths       []string  `yaml:"paths"`
	CITargets   []string  `yaml:"ci_targets"`
	PROrder     int       `yaml:"pr_order"`
	Status      string    `yaml:"status"`
	Notes       yaml.Node `yaml:"notes,omitempty"`
	Acceptance  []string  `yaml:"acceptance"`
}

func TestTaskManifestIsStrictAndDependencyOrdered(t *testing.T) {
	source, err := os.ReadFile("tasks.yaml")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeManifest(source)
	if err != nil {
		t.Fatalf("tasks.yaml is not a strict manifest: %v", err)
	}
	if err := validateManifest(manifest); err != nil {
		t.Fatalf("tasks.yaml violates task graph invariants: %v", err)
	}
}

func TestTaskManifestRejectsUnknownFieldsAndBrokenGraph(t *testing.T) {
	tests := map[string]string{
		"unknown field": `
version: 1
feature: sparerunner
tasks: []
unexpected: true
`,
		"unknown dependency": `
version: 1
feature: sparerunner
tasks:
  - id: spr-001
    title: first
    type: foundation
    description: first task
    depends_on: [spr-999]
    paths: [spec]
    ci_targets: [test]
    pr_order: 1
    status: todo
    acceptance: [validated]
`,
		"duplicate task": `
version: 1
feature: sparerunner
tasks:
  - &task
    id: spr-001
    title: first
    type: foundation
    description: first task
    depends_on: []
    paths: [spec]
    ci_targets: [test]
    pr_order: 1
    status: todo
    acceptance: [validated]
  - *task
`,
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			manifest, err := decodeManifest([]byte(source))
			if err == nil {
				err = validateManifest(manifest)
			}
			if err == nil {
				t.Fatal("invalid task manifest accepted")
			}
		})
	}
}

func decodeManifest(source []byte) (taskManifest, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	decoder.KnownFields(true)
	var manifest taskManifest
	if err := decoder.Decode(&manifest); err != nil {
		return taskManifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return taskManifest{}, errors.New("multiple YAML documents are not allowed")
		}
		return taskManifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest taskManifest) error {
	if manifest.Version != 1 || manifest.Feature != "sparerunner" || len(manifest.Tasks) == 0 {
		return errors.New("manifest requires version 1, feature sparerunner, and at least one task")
	}
	byID := make(map[string]taskSpec, len(manifest.Tasks))
	byOrder := make(map[int]string, len(manifest.Tasks))
	for _, task := range manifest.Tasks {
		if !taskIDPattern.MatchString(task.ID) {
			return fmt.Errorf("invalid task ID %q", task.ID)
		}
		if task.Title == "" || task.Type == "" || task.Description == "" || len(task.Paths) == 0 || len(task.CITargets) == 0 || len(task.Acceptance) == 0 {
			return fmt.Errorf("task %s is incomplete", task.ID)
		}
		switch task.Status {
		case "todo", "in_progress", "done":
		default:
			return fmt.Errorf("task %s has invalid status %q", task.ID, task.Status)
		}
		if _, exists := byID[task.ID]; exists {
			return fmt.Errorf("duplicate task ID %s", task.ID)
		}
		if existing, exists := byOrder[task.PROrder]; exists || task.PROrder < 1 {
			return fmt.Errorf("task %s has invalid or duplicate PR order with %s", task.ID, existing)
		}
		byID[task.ID] = task
		byOrder[task.PROrder] = task.ID
	}
	for _, task := range manifest.Tasks {
		for _, dependencyID := range task.DependsOn {
			dependency, exists := byID[dependencyID]
			if !exists {
				return fmt.Errorf("task %s depends on unknown task %s", task.ID, dependencyID)
			}
			if dependency.PROrder >= task.PROrder {
				return fmt.Errorf("task %s dependency %s is not ordered before it", task.ID, dependencyID)
			}
		}
	}
	return nil
}
