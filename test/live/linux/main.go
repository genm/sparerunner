package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := runCLI(ctx, os.Args[1:], logger); err != nil {
		logger.Error(
			"linux_live_acceptance_failed",
			slog.String("error_class", classifyLiveError(err)),
		)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string, logger *slog.Logger) error {
	if len(args) == 0 {
		return errConfigInvalid
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	switch args[0] {
	case "validate-config":
		flags := flag.NewFlagSet("validate-config", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		_, err := loadLiveConfig(configPath)
		return err
	case "controller":
		flags := flag.NewFlagSet("controller", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, rawMode string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		flags.StringVar(&rawMode, "mode", "", "acceptance scenario")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		mode, err := parseAcceptanceMode(rawMode)
		if err != nil {
			return err
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		_, err = runLiveAcceptance(ctx, config, mode, logger)
		return err
	case "capture-node":
		flags := flag.NewFlagSet("capture-node", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var phase, configPath string
		flags.StringVar(&phase, "phase", "", "before, running-before-restart, running-after-restart, or after")
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return captureNodeEvidence(phase, config, liveAuthorityProbe{})
	case "capture-authority":
		flags := flag.NewFlagSet("capture-authority", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, repoRoot string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		flags.StringVar(&repoRoot, "repo-root", "", "absolute clean repository root")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return captureAuthorityEvidence(config, repoRoot, liveAuthorityProbe{})
	case "prepare-injector":
		flags := flag.NewFlagSet("prepare-injector", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, source string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		flags.StringVar(&source, "source", "", "absolute root-owned injector source")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return prepareInjector(config, source)
	case "exec-injector":
		flags := flag.NewFlagSet("exec-injector", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, operation string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		flags.StringVar(&operation, "operation", "", "arm or disarm")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return executeInjector(config, operation)
	case "cleanup-injector":
		flags := flag.NewFlagSet("cleanup-injector", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return cleanupInjectorCopy(config)
	case "validate-evidence":
		flags := flag.NewFlagSet("validate-evidence", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath, rawMode string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		flags.StringVar(&rawMode, "mode", "", "acceptance scenario")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		mode, err := parseAcceptanceMode(rawMode)
		if err != nil {
			return err
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		return validateFinalEvidence(config, mode, time.Now().UTC())
	case "record-ack-gate-kill":
		flags := flag.NewFlagSet("record-ack-gate-kill", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		var configPath string
		flags.StringVar(&configPath, "config", "", "absolute path to the non-secret live config")
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
			return errConfigInvalid
		}
		config, err := loadLiveConfig(configPath)
		if err != nil {
			return err
		}
		evidence, err := openEvidenceStore(config.EvidenceDirectory)
		if err != nil {
			return err
		}
		return evidence.recordAckGateKill(time.Now().UTC())
	default:
		return errors.Join(errConfigInvalid, errors.New("unknown live acceptance command"))
	}
}
