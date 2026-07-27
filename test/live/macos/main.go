package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func main() {
	if handled, exitCode := runPrivateMaterialProbe(os.Args[1:]); handled {
		os.Exit(exitCode)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := runMacOSLiveCLI(ctx, os.Args[1:], logger); err != nil {
		logger.Error(
			"macos_live_acceptance_failed",
			slog.String("error_class", classifyMacOSLiveError(err)),
		)
		os.Exit(1)
	}
}

func runMacOSLiveCLI(
	ctx context.Context,
	args []string,
	logger *slog.Logger,
) error {
	if len(args) == 0 {
		return errMacOSConfigInvalid
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	switch args[0] {
	case "validate-config":
		flags := flag.NewFlagSet("validate-config", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath string
		flags.StringVar(&configPath, "config", "", "absolute non-secret config path")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return errMacOSConfigInvalid
		}
		_, err := loadMacOSLiveConfig(configPath)
		return err
	case "capture":
		flags := flag.NewFlagSet("capture", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, rawPhase string
		flags.StringVar(&configPath, "config", "", "absolute non-secret config path")
		flags.StringVar(&rawPhase, "phase", "", "capture phase")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return errMacOSConfigInvalid
		}
		config, err := loadMacOSLiveConfig(configPath)
		if err != nil {
			return err
		}
		phase, err := parseCapturePhase(rawPhase)
		if err != nil {
			return err
		}
		runCtx, cancel := context.WithTimeout(
			ctx,
			time.Duration(config.MaximumRunSeconds)*time.Second,
		)
		defer cancel()
		evidence, err := captureMacOSNode(runCtx, config, phase)
		if err != nil {
			return err
		}
		store, err := openEvidenceStore(config.EvidenceDirectory)
		if err != nil {
			return err
		}
		return store.writeNode(evidence)
	case "validate":
		flags := flag.NewFlagSet("validate", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, rawScenario string
		flags.StringVar(&configPath, "config", "", "absolute non-secret config path")
		flags.StringVar(&rawScenario, "scenario", "", "normal, sleep, or reboot")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return errMacOSConfigInvalid
		}
		config, err := loadMacOSLiveConfig(configPath)
		if err != nil {
			return err
		}
		scenario, err := parseAcceptanceScenario(rawScenario)
		if err != nil {
			return err
		}
		store, err := openEvidenceStore(config.EvidenceDirectory)
		if err != nil {
			return err
		}
		return validateScenario(config, scenario, time.Now().UTC(), store)
	default:
		return errors.Join(errMacOSConfigInvalid, errors.New("unknown command"))
	}
}
