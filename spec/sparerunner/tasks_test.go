package sparerunner_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// A task ID is an opaque handle, so the pattern carries no product name and no
// ordering. Execution order is the depends_on graph alone, which is why this
// file enforces acyclicity instead of a hand-maintained order field.
var taskIDPattern = regexp.MustCompile(`^task-[0-9]{3}$`)

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
  - id: task-001
    title: first
    type: foundation
    description: first task
    depends_on: [task-999]
    paths: [spec]
    ci_targets: [test]
    status: todo
    acceptance: [validated]
`,
		"duplicate task": `
version: 1
feature: sparerunner
tasks:
  - &task
    id: task-001
    title: first
    type: foundation
    description: first task
    depends_on: []
    paths: [spec]
    ci_targets: [test]
    status: todo
    acceptance: [validated]
  - *task
`,
		"dependency cycle": `
version: 1
feature: sparerunner
tasks:
  - id: task-001
    title: first
    type: foundation
    description: first task
    depends_on: [task-002]
    paths: [spec]
    ci_targets: [test]
    status: todo
    acceptance: [validated]
  - id: task-002
    title: second
    type: foundation
    description: second task
    depends_on: [task-001]
    paths: [spec]
    ci_targets: [test]
    status: todo
    acceptance: [validated]
`,
		"self dependency": `
version: 1
feature: sparerunner
tasks:
  - id: task-001
    title: first
    type: foundation
    description: first task
    depends_on: [task-001]
    paths: [spec]
    ci_targets: [test]
    status: todo
    acceptance: [validated]
`,
		"retired pr_order field": `
version: 1
feature: sparerunner
tasks:
  - id: task-001
    title: first
    type: foundation
    description: first task
    depends_on: []
    paths: [spec]
    ci_targets: [test]
    pr_order: 1
    status: todo
    acceptance: [validated]
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
		byID[task.ID] = task
	}
	for _, task := range manifest.Tasks {
		for _, dependencyID := range task.DependsOn {
			if _, exists := byID[dependencyID]; !exists {
				return fmt.Errorf("task %s depends on unknown task %s", task.ID, dependencyID)
			}
		}
	}
	return validateAcyclic(manifest.Tasks, byID)
}

// validateAcyclic proves the graph can be executed at all. It replaces the
// former pr_order field: a hand-maintained total order duplicated what
// depends_on already expresses, forced a renumber whenever a task was inserted,
// and made the ID look like a position it never carried.
func validateAcyclic(tasks []taskSpec, byID map[string]taskSpec) error {
	// The zero value is "unvisited", so an absent map entry needs no special case.
	const (
		onStack = iota + 1
		settled
	)
	state := make(map[string]int, len(tasks))
	var path []string
	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case settled:
			return nil
		case onStack:
			return fmt.Errorf("task dependency cycle: %s -> %s", strings.Join(path, " -> "), id)
		}
		state[id] = onStack
		path = append(path, id)
		for _, dependencyID := range byID[id].DependsOn {
			if err := visit(dependencyID); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		state[id] = settled
		return nil
	}
	for _, task := range tasks {
		if err := visit(task.ID); err != nil {
			return err
		}
	}
	return nil
}
