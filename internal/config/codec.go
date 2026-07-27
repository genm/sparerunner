package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"go.yaml.in/yaml/v3"
)

func DecodeJSON(reader io.Reader) (Configuration, error) {
	body, err := readConfigurationBody(reader)
	if err != nil {
		if errors.Is(err, ErrPayloadTooLarge) {
			return Configuration{}, err
		}
		return Configuration{}, ErrInvalidJSON
	}
	if err := validateJSONDocument(body); err != nil {
		return Configuration{}, ErrInvalidJSON
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var configuration Configuration
	if err := decoder.Decode(&configuration); err != nil {
		return Configuration{}, ErrInvalidJSON
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Configuration{}, ErrInvalidJSON
	}
	if err := configuration.Validate(); err != nil {
		return Configuration{}, err
	}
	return configuration, nil
}

func DecodeYAML(reader io.Reader) (Configuration, error) {
	body, err := readConfigurationBody(reader)
	if err != nil {
		if errors.Is(err, ErrPayloadTooLarge) {
			return Configuration{}, err
		}
		return Configuration{}, ErrInvalidYAML
	}

	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	var configuration Configuration
	if err := decoder.Decode(&configuration); err != nil {
		return Configuration{}, ErrInvalidYAML
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Configuration{}, ErrInvalidYAML
	}

	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil ||
		validateYAMLDocument(&document) != nil {
		return Configuration{}, ErrInvalidYAML
	}
	if err := configuration.Validate(); err != nil {
		return Configuration{}, err
	}
	return configuration, nil
}

func EncodeJSON(configuration Configuration) ([]byte, error) {
	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(canonicalConfiguration(configuration))
}

func EncodeYAML(configuration Configuration) ([]byte, error) {
	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(canonicalConfiguration(configuration)); err != nil {
		return nil, ErrInvalidYAML
	}
	if err := encoder.Close(); err != nil {
		return nil, ErrInvalidYAML
	}
	return output.Bytes(), nil
}

func (value DecimalUint64) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, strconv.FormatUint(uint64(value), 10)), nil
}

func (value *DecimalUint64) UnmarshalJSON(data []byte) error {
	if value == nil {
		return ErrInvalidJSON
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return ErrInvalidJSON
	}
	parsed, ok := parseCanonicalUint64(text)
	if !ok {
		return ErrInvalidJSON
	}
	*value = DecimalUint64(parsed)
	return nil
}

func (value DecimalUint64) MarshalYAML() (any, error) {
	return uint64(value), nil
}

func (value *DecimalUint64) UnmarshalYAML(node *yaml.Node) error {
	if value == nil ||
		node == nil ||
		node.Kind != yaml.ScalarNode ||
		node.Tag != "!!int" {
		return ErrInvalidYAML
	}
	parsed, ok := parseCanonicalUint64(node.Value)
	if !ok {
		return ErrInvalidYAML
	}
	*value = DecimalUint64(parsed)
	return nil
}

func readConfigurationBody(reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, ErrInvalidConfiguration
	}
	limited := &io.LimitedReader{
		R: reader,
		N: RequestBodyLimitBytes + 1,
	}
	body, err := io.ReadAll(limited)
	if int64(len(body)) > RequestBodyLimitBytes {
		return nil, ErrPayloadTooLarge
	}
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return body, nil
}

func parseCanonicalUint64(text string) (uint64, bool) {
	if text == "0" {
		return 0, true
	}
	if text == "" || text[0] < '1' || text[0] > '9' {
		return 0, false
	}
	for index := 1; index < len(text); index++ {
		if text[index] < '0' || text[index] > '9' {
			return 0, false
		}
	}
	value, err := strconv.ParseUint(text, 10, 64)
	return value, err == nil
}

func validateJSONDocument(body []byte) error {
	if err := rejectDuplicateJSONKeys(body); err != nil {
		return err
	}
	root, err := decodeJSONObject(
		body,
		stringSet(
			"schemaVersion",
			"revision",
			"scheduler",
			"nodes",
			"runnerProfiles",
			"targets",
		),
		stringSet(
			"schemaVersion",
			"revision",
			"scheduler",
			"nodes",
			"runnerProfiles",
			"targets",
		),
	)
	if err != nil {
		return err
	}
	if _, err := decodeJSONObject(
		root["scheduler"],
		stringSet("maxRunners"),
		nil,
	); err != nil {
		return err
	}
	if err := validateJSONArrayObjects(
		root["nodes"],
		stringSet("id", "displayName", "maxRunners"),
		stringSet("id", "displayName", "maxRunners"),
	); err != nil {
		return err
	}
	if err := validateJSONArrayObjects(
		root["runnerProfiles"],
		stringSet(
			"id",
			"label",
			"operatingSystem",
			"architecture",
			"minAvailableMemoryBytes",
			"versionPolicy",
			"runtime",
		),
		stringSet(
			"id",
			"label",
			"minAvailableMemoryBytes",
			"versionPolicy",
			"runtime",
		),
	); err != nil {
		return err
	}
	return validateJSONArrayObjects(
		root["targets"],
		stringSet(
			"id",
			"installationId",
			"scopeKind",
			"scope",
			"scaleSetName",
			"runnerProfileId",
		),
		stringSet(
			"id",
			"installationId",
			"scopeKind",
			"scope",
			"scaleSetName",
			"runnerProfileId",
		),
	)
}

func rejectDuplicateJSONKeys(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidJSON
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return ErrInvalidJSON
			}
			if _, duplicate := keys[key]; duplicate {
				return ErrInvalidJSON
			}
			keys[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return ErrInvalidJSON
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return ErrInvalidJSON
		}
	default:
		return ErrInvalidJSON
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidJSON
	}
	return nil
}

func decodeJSONObject(
	raw []byte,
	allowed map[string]struct{},
	required map[string]struct{},
) (map[string]json.RawMessage, error) {
	if firstNonSpace(raw) != '{' {
		return nil, ErrInvalidJSON
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, ErrInvalidJSON
	}
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return nil, ErrInvalidJSON
		}
		if firstNonSpace(object[key]) == 'n' {
			return nil, ErrInvalidJSON
		}
	}
	for key := range required {
		if _, ok := object[key]; !ok {
			return nil, ErrInvalidJSON
		}
	}
	return object, nil
}

func validateJSONArrayObjects(
	raw []byte,
	allowed map[string]struct{},
	required map[string]struct{},
) error {
	if firstNonSpace(raw) != '[' {
		return ErrInvalidJSON
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || items == nil {
		return ErrInvalidJSON
	}
	for _, item := range items {
		if _, err := decodeJSONObject(item, allowed, required); err != nil {
			return err
		}
	}
	return nil
}

func firstNonSpace(raw []byte) byte {
	for _, value := range raw {
		switch value {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return value
		}
	}
	return 0
}

func validateYAMLDocument(document *yaml.Node) error {
	if document == nil ||
		document.Kind != yaml.DocumentNode ||
		len(document.Content) != 1 {
		return ErrInvalidYAML
	}
	root, err := decodeYAMLMapping(
		document.Content[0],
		stringSet(
			"schemaVersion",
			"revision",
			"scheduler",
			"nodes",
			"runnerProfiles",
			"targets",
		),
		stringSet(
			"schemaVersion",
			"revision",
			"scheduler",
			"nodes",
			"runnerProfiles",
			"targets",
		),
	)
	if err != nil {
		return err
	}
	if err := requireYAMLScalarTags(root, map[string]string{
		"schemaVersion": "!!int",
		"revision":      "!!int",
	}); err != nil {
		return err
	}
	scheduler, err := decodeYAMLMapping(
		root["scheduler"],
		stringSet("maxRunners"),
		nil,
	)
	if err != nil {
		return err
	}
	if err := requireYAMLScalarTags(scheduler, map[string]string{
		"maxRunners": "!!int",
	}); err != nil {
		return err
	}
	if err := validateYAMLSequenceMappings(
		root["nodes"],
		stringSet("id", "displayName", "maxRunners"),
		stringSet("id", "displayName", "maxRunners"),
		map[string]string{
			"id":          "!!str",
			"displayName": "!!str",
			"maxRunners":  "!!int",
		},
	); err != nil {
		return err
	}
	if err := validateYAMLSequenceMappings(
		root["runnerProfiles"],
		stringSet(
			"id",
			"label",
			"operatingSystem",
			"architecture",
			"minAvailableMemoryBytes",
			"versionPolicy",
			"runtime",
		),
		stringSet(
			"id",
			"label",
			"minAvailableMemoryBytes",
			"versionPolicy",
			"runtime",
		),
		map[string]string{
			"id":                      "!!str",
			"label":                   "!!str",
			"operatingSystem":         "!!str",
			"architecture":            "!!str",
			"minAvailableMemoryBytes": "!!int",
			"versionPolicy":           "!!str",
			"runtime":                 "!!str",
		},
	); err != nil {
		return err
	}
	return validateYAMLSequenceMappings(
		root["targets"],
		stringSet(
			"id",
			"installationId",
			"scopeKind",
			"scope",
			"scaleSetName",
			"runnerProfileId",
		),
		stringSet(
			"id",
			"installationId",
			"scopeKind",
			"scope",
			"scaleSetName",
			"runnerProfileId",
		),
		map[string]string{
			"id":              "!!str",
			"installationId":  "!!str",
			"scopeKind":       "!!str",
			"scope":           "!!str",
			"scaleSetName":    "!!str",
			"runnerProfileId": "!!str",
		},
	)
}

func decodeYAMLMapping(
	node *yaml.Node,
	allowed map[string]struct{},
	required map[string]struct{},
) (map[string]*yaml.Node, error) {
	node = resolveYAMLAlias(node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, ErrInvalidYAML
	}
	object := make(map[string]*yaml.Node, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		keyNode := resolveYAMLAlias(node.Content[index])
		if keyNode == nil || keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return nil, ErrInvalidYAML
		}
		if _, ok := allowed[keyNode.Value]; !ok {
			return nil, ErrInvalidYAML
		}
		if _, duplicate := object[keyNode.Value]; duplicate {
			return nil, ErrInvalidYAML
		}
		valueNode := resolveYAMLAlias(node.Content[index+1])
		if valueNode == nil || valueNode.Tag == "!!null" {
			return nil, ErrInvalidYAML
		}
		object[keyNode.Value] = valueNode
	}
	for key := range required {
		if _, ok := object[key]; !ok {
			return nil, ErrInvalidYAML
		}
	}
	return object, nil
}

func validateYAMLSequenceMappings(
	node *yaml.Node,
	allowed map[string]struct{},
	required map[string]struct{},
	scalarTags map[string]string,
) error {
	node = resolveYAMLAlias(node)
	if node == nil || node.Kind != yaml.SequenceNode {
		return ErrInvalidYAML
	}
	for _, item := range node.Content {
		object, err := decodeYAMLMapping(item, allowed, required)
		if err != nil {
			return err
		}
		if err := requireYAMLScalarTags(object, scalarTags); err != nil {
			return err
		}
	}
	return nil
}

func requireYAMLScalarTags(
	object map[string]*yaml.Node,
	expected map[string]string,
) error {
	for field, tag := range expected {
		node, exists := object[field]
		if !exists {
			continue
		}
		node = resolveYAMLAlias(node)
		if node == nil || node.Kind != yaml.ScalarNode || node.Tag != tag {
			return ErrInvalidYAML
		}
	}
	return nil
}

func resolveYAMLAlias(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
