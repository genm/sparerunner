//go:build !windows

package main

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type fakeAuthorityProbe struct {
	properties      map[string]string
	unitContents    map[string][]byte
	uids            map[string]int
	files           map[string][]byte
	executables     map[int][2]string
	fileDigests     map[string]string
	buildVCS        map[string][2]string
	runnerAuthority [3]string
	trustedFiles    map[string]bool
	commit          string
	clean           bool
}

func (probe fakeAuthorityProbe) systemdUnitContent(unit string) ([]byte, error) {
	value, ok := probe.unitContents[unit]
	if !ok {
		return nil, errNodeEvidenceInvalid
	}
	return value, nil
}

func (probe fakeAuthorityProbe) gitState(string) (string, bool, error) {
	return probe.commit, probe.clean, nil
}

func (probe fakeAuthorityProbe) executableIdentity(pid int) (string, string, error) {
	value, ok := probe.executables[pid]
	if !ok {
		return "", "", errNodeEvidenceInvalid
	}
	return value[0], value[1], nil
}

func (probe fakeAuthorityProbe) regularFileDigest(path string) (string, error) {
	value, ok := probe.fileDigests[path]
	if !ok {
		return "", errNodeEvidenceInvalid
	}
	return value, nil
}

func (probe fakeAuthorityProbe) goBuildVCS(path string) (string, bool, error) {
	value, ok := probe.buildVCS[path]
	if !ok {
		return "", false, errNodeEvidenceInvalid
	}
	return value[0], value[1] == "true", nil
}

func (probe fakeAuthorityProbe) officialRunnerAuthority(string) (string, string, int64, error) {
	size, err := strconv.ParseInt(probe.runnerAuthority[2], 10, 64)
	if err != nil {
		return "", "", 0, errNodeEvidenceInvalid
	}
	return probe.runnerAuthority[0], probe.runnerAuthority[1], size, nil
}

func (probe fakeAuthorityProbe) trustedRootFile(path string) error {
	if !canonicalAbsolutePath(path) || !probe.trustedFiles[path] {
		return errNodeEvidenceInvalid
	}
	return nil
}

func (probe fakeAuthorityProbe) systemdProperty(unit, property string) (string, error) {
	value, ok := probe.properties[unit+"\x00"+property]
	if !ok {
		return "", errNodeEvidenceInvalid
	}
	return value, nil
}

func (probe fakeAuthorityProbe) lookupUID(name string) (int, error) {
	value, ok := probe.uids[name]
	if !ok {
		return 0, errNodeEvidenceInvalid
	}
	return value, nil
}

func (probe fakeAuthorityProbe) readFile(path string) ([]byte, error) {
	value, ok := probe.files[path]
	if !ok {
		return nil, errNodeEvidenceInvalid
	}
	return value, nil
}

func TestReadServiceAuthorityBindsEffectiveRuntimeMainPIDUIDAndCgroup(t *testing.T) {
	probe := validAuthorityProbe()
	got, err := readServiceAuthority(
		probe,
		"tewake-agent.service",
		"serve",
		"/var/lib/tewake-runtime",
		"/etc/systemd/system/tewake-agent.service",
		1001,
	)
	if err != nil {
		t.Fatalf("readServiceAuthority() error = %v", err)
	}
	if got.MainPID != 101 || got.UID != 1001 ||
		got.ControlGroup != "/system.slice/tewake-agent.service" ||
		got.ProcessStartTicks != 111 {
		t.Fatalf("authority = %#v", got)
	}
}

func TestReadServiceAuthorityRejectsDecoyRuntimeFakePIDWrongUIDAndPIDReuse(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*fakeAuthorityProbe)
	}{
		{name: "decoy runtime root", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] =
				strings.Replace(
					probe.properties["tewake-agent.service\x00ExecStart"],
					"--runtime-root=/var/lib/tewake-runtime",
					"--runtime-root=/var/lib/tewake-runtime-decoy",
					1,
				)
		}},
		{name: "fake main pid", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00MainPID"] = "999"
		}},
		{name: "wrong uid", mutate: func(probe *fakeAuthorityProbe) {
			probe.files["/proc/101/status"] = fakeStatus(1002)
		}},
		{name: "wrong cgroup", mutate: func(probe *fakeAuthorityProbe) {
			probe.files["/proc/101/cgroup"] = []byte("0::/user.slice/decoy\n")
		}},
		{name: "missing start identity", mutate: func(probe *fakeAuthorityProbe) {
			probe.files["/proc/101/stat"] = []byte("101 (tewake-agent) S 1 2\n")
		}},
		{name: "fake argv executable", mutate: func(probe *fakeAuthorityProbe) {
			probe.executables[101] = [2]string{"/tmp/tewake-agent", strings.Repeat("a", 64)}
		}},
		{name: "unexpected state directory", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] = strings.Replace(
				probe.properties["tewake-agent.service\x00ExecStart"],
				"--state-dir=/var/lib/tewake-agent",
				"--state-dir=/var/lib/decoy",
				1,
			)
		}},
		{name: "unexpected cache directory", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] = strings.Replace(
				probe.properties["tewake-agent.service\x00ExecStart"],
				"--cache-root=/var/cache/tewake-agent",
				"--cache-root=/var/cache/decoy",
				1,
			)
		}},
		{name: "wrong supervisor socket", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] = strings.Replace(
				probe.properties["tewake-agent.service\x00ExecStart"],
				"--supervisor-socket=/run/tewake-supervisor/supervisor.sock",
				"--supervisor-socket=/run/decoy.sock",
				1,
			)
		}},
		{name: "duplicate binary", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] +=
				" /usr/local/bin/tewake-agent"
		}},
		{name: "unexpected flag", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00ExecStart"] = strings.Replace(
				probe.properties["tewake-agent.service\x00ExecStart"],
				" ; ignore_errors",
				" --unexpected ; ignore_errors",
				1,
			)
		}},
		{name: "untrusted drop in", mutate: func(probe *fakeAuthorityProbe) {
			probe.properties["tewake-agent.service\x00DropInPaths"] =
				"/run/user/1001/decoy.conf"
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			probe := validAuthorityProbe()
			testCase.mutate(&probe)
			if _, err := readServiceAuthority(
				probe,
				"tewake-agent.service",
				"serve",
				"/var/lib/tewake-runtime",
				"/etc/systemd/system/tewake-agent.service",
				1001,
			); !errors.Is(err, errNodeEvidenceInvalid) {
				t.Fatalf("readServiceAuthority() error = %v, want errNodeEvidenceInvalid", err)
			}
		})
	}
}

func TestReadSupervisorAuthorityRejectsWrongSocketFenceAndUsers(t *testing.T) {
	for _, replacement := range [][2]string{
		{"--socket=/run/tewake-supervisor/supervisor.sock", "--socket=/run/decoy.sock"},
		{"--fence-root=/var/lib/tewake-supervisor/fences", "--fence-root=/var/lib/decoy"},
		{"--runner-user=tewake-runner-0", "--runner-user=root"},
		{"--agent-user=tewake-agent", "--agent-user=root"},
	} {
		probe := validAuthorityProbe()
		key := "tewake-supervisor.service\x00ExecStart"
		probe.properties[key] = strings.Replace(
			probe.properties[key],
			replacement[0],
			replacement[1],
			1,
		)
		if _, err := readServiceAuthority(
			probe,
			"tewake-supervisor.service",
			"supervisor",
			"/var/lib/tewake-runtime",
			"/etc/systemd/system/tewake-supervisor.service",
			0,
		); !errors.Is(err, errNodeEvidenceInvalid) {
			t.Fatalf(
				"replacement %q error = %v, want errNodeEvidenceInvalid",
				replacement,
				err,
			)
		}
	}
}

func TestParseEffectiveExecStartRejectsDuplicateAndPrefixRuntimeArguments(t *testing.T) {
	for _, value := range []string{
		"{ /usr/local/bin/tewake-agent ; /usr/local/bin/tewake-agent serve --runtime-root=/runtime --runtime-root=/decoy ; }",
		"{ /usr/local/bin/tewake-agent ; /usr/local/bin/tewake-agent serve --runtime-root=/runtime-extra ; }",
		"{ /tmp/tewake-agent ; /tmp/tewake-agent serve --runtime-root=/runtime ; }",
	} {
		argv, err := parseEffectiveExecStart(value)
		if err == nil &&
			stringSlicesEqual(argv, expectedServiceArgv("serve", "/runtime")) {
			t.Fatalf("unsafe ExecStart accepted: %q", value)
		}
	}
}

func TestParseEffectiveExecStartAcceptsRealSystemdShowSerialization(t *testing.T) {
	expected := expectedServiceArgv("serve", "/var/lib/tewake-runtime")
	value := "{ path=/usr/local/bin/tewake-agent ; argv[]=" +
		strings.Join(expected, " ") +
		" ; ignore_errors=no ; start_time=[Sun 2026-07-27 12:00:00 JST] ; " +
		"stop_time=[n/a] ; pid=101 ; code=(null) ; status=0/0 }"
	actual, err := parseEffectiveExecStart(value)
	if err != nil || !stringSlicesEqual(actual, expected) {
		t.Fatalf("parseEffectiveExecStart() = %#v, %v", actual, err)
	}
	for _, unsafe := range []string{
		strings.Replace(value, " ; pid=101", " ; path=/tmp/decoy ; pid=101", 1),
		strings.Replace(value, " ; pid=101", " ; unexpected=value ; pid=101", 1),
	} {
		if _, err := parseEffectiveExecStart(unsafe); !errors.Is(err, errNodeEvidenceInvalid) {
			t.Fatalf("unsafe metadata accepted: %q", unsafe)
		}
	}
}

func validAuthorityProbe() fakeAuthorityProbe {
	properties := make(map[string]string)
	files := map[string][]byte{
		"/proc/sys/kernel/random/boot_id": []byte("01234567-89ab-cdef-0123-456789abcdef\n"),
	}
	for _, service := range []struct {
		unit, command, user string
		pid                 int
		uid                 int
		cgroup              string
		ticks               uint64
	}{
		{"tewake-agent.service", "serve", "tewake-agent", 101, 1001, "/system.slice/tewake-agent.service", 111},
		{"tewake-supervisor.service", "supervisor", "root", 202, 0, "/system.slice/tewake-supervisor.service", 222},
	} {
		properties[service.unit+"\x00ExecStart"] =
			"{ path=/usr/local/bin/tewake-agent ; argv[]=" +
				strings.Join(expectedServiceArgv(service.command, "/var/lib/tewake-runtime"), " ") +
				" ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
		properties[service.unit+"\x00MainPID"] = strconv.Itoa(service.pid)
		properties[service.unit+"\x00ControlGroup"] = service.cgroup
		properties[service.unit+"\x00FragmentPath"] =
			"/etc/systemd/system/" + service.unit
		properties[service.unit+"\x00DropInPaths"] = ""
		files[filepath.Join("/proc", strconv.Itoa(service.pid), "status")] = fakeStatus(service.uid)
		files[filepath.Join("/proc", strconv.Itoa(service.pid), "stat")] = fakeStat(service.pid, service.ticks)
		files[filepath.Join("/proc", strconv.Itoa(service.pid), "cgroup")] = []byte("0::" + service.cgroup + "\n")
	}
	return fakeAuthorityProbe{
		properties: properties,
		unitContents: map[string][]byte{
			"tewake-agent.service":      []byte("agent unit\n"),
			"tewake-supervisor.service": []byte("supervisor unit\n"),
		},
		uids: map[string]int{
			"tewake-agent":    1001,
			"tewake-runner-0": 1002,
		},
		files: files,
		executables: map[int][2]string{
			101: {"/usr/local/bin/tewake-agent", strings.Repeat("a", 64)},
			202: {"/usr/local/bin/tewake-agent", strings.Repeat("a", 64)},
		},
		fileDigests: map[string]string{
			"/usr/local/bin/tewake-agent": strings.Repeat("a", 64),
		},
		buildVCS: map[string][2]string{
			"/usr/local/bin/tewake-agent": {strings.Repeat("1", 40), "false"},
		},
		runnerAuthority: [3]string{
			"/var/lib/tewake-runtime/.tewake-official/test/archive",
			strings.Repeat("5", 64),
			"1",
		},
		trustedFiles: map[string]bool{
			"/etc/systemd/system/tewake-agent.service":      true,
			"/etc/systemd/system/tewake-supervisor.service": true,
		},
		commit: strings.Repeat("1", 40),
		clean:  true,
	}
}

func fakeStatus(uid int) []byte {
	return []byte("Name:\ttest\nUid:\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\t" + strconv.Itoa(uid) + "\n")
}

func fakeStat(pid int, ticks uint64) []byte {
	fields := make([]string, 20)
	for index := range fields {
		fields[index] = "1"
	}
	fields[0] = "S"
	fields[19] = strconv.FormatUint(ticks, 10)
	return []byte(strconv.Itoa(pid) + " (process name) " + strings.Join(fields, " ") + "\n")
}
