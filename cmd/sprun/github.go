package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/genm/sparerunner/internal/app"
	"github.com/genm/sparerunner/internal/github"
	"github.com/spf13/cobra"
)

// The GitHub App credential has two entry points: this CLI, which needs no
// browser and no management session, and the optional Web UI Manifest flow.
// Both write through the same controller-owned platform credential store, so
// neither is a privileged path. The key is accepted only as a file path — never
// as a flag value — because a command-line argument is visible to every process
// on the host and lands in shell history.

const githubAppCredentialFile = "github-app-credential.json"

// maximumAppKeyFileBytes bounds the PEM read. It matches the App key bound the
// github package already enforces, so a larger file is rejected before it is
// read into memory rather than after.
const maximumAppKeyFileBytes = 128 << 10

func newGitHubCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "github",
		Short: "Connect and inspect the GitHub App this controller acts as",
	}
	command.AddCommand(newGitHubConnectCommand())
	command.AddCommand(newGitHubInstallationsCommand())
	return command
}

func newGitHubConnectCommand() *cobra.Command {
	var stateDirectory, clientID, privateKeyFile string
	var appID int64
	command := &cobra.Command{
		Use:   "connect",
		Short: "Store the credentials of a GitHub App you already created",
		Long: "Store the credentials of a GitHub App you already created.\n\n" +
			"Create the App yourself at https://github.com/settings/apps/new (or your\n" +
			"organization's equivalent page), then pass its identifiers and the private\n" +
			"key file you downloaded. The key is read from the file and handed to this\n" +
			"host's credential store; it is never accepted as a flag value.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveControllerStateDirectory(stateDirectory)
			if err != nil {
				return err
			}
			credential, err := readAppCredential(appID, clientID, privateKeyFile)
			if err != nil {
				return err
			}
			store := github.NewPlatformAppCredentialStore(
				filepath.Join(directory, githubAppCredentialFile))
			// The store itself is no-clobber, so this read only turns "a
			// different App is already connected" into an actionable message
			// instead of an unavailable-store error. It is deliberately not the
			// safety boundary: the store still refuses the overwrite.
			existing, present, loadErr := store.Load()
			if loadErr != nil {
				return loadErr
			}
			if present && existing.AppID != credential.AppID {
				return fmt.Errorf(
					"this controller is already connected to GitHub App %d; "+
						"disconnect it before connecting App %d",
					existing.AppID, credential.AppID,
				)
			}
			if err := store.Save(credential); err != nil {
				return err
			}
			fmt.Fprintf(
				command.OutOrStdout(),
				"Connected GitHub App %d. Install it into your organizations, then run: "+
					"sprun github installations\n",
				credential.AppID,
			)
			fmt.Fprintf(
				command.OutOrStdout(),
				"The private key is now held by this host's credential store; "+
					"you can delete %s.\n",
				privateKeyFile,
			)
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	command.Flags().Int64Var(&appID, "app-id", 0, "numeric App ID from the App's settings page")
	command.Flags().StringVar(&clientID, "client-id", "", "client ID from the App's settings page")
	command.Flags().StringVar(&privateKeyFile, "private-key-file", "", "path to the PEM private key downloaded from GitHub")
	return command
}

func newGitHubInstallationsCommand() *cobra.Command {
	var stateDirectory string
	command := &cobra.Command{
		Use:   "installations",
		Short: "List the accounts the connected GitHub App is installed into",
		Long: "List the accounts the connected GitHub App is installed into.\n\n" +
			"The reported installation ID is what a Target's installationId field in\n" +
			"the configuration document refers to.",
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			directory, err := resolveControllerStateDirectory(stateDirectory)
			if err != nil {
				return err
			}
			authority, err := github.NewAuthority(github.AuthorityOptions{
				CredentialStore: github.NewPlatformAppCredentialStore(
					filepath.Join(directory, githubAppCredentialFile)),
			})
			if err != nil {
				return err
			}
			installations, err := authority.ListInstallations(command.Context())
			if err != nil {
				if errors.Is(err, github.ErrGitHubNotConnected) {
					return errors.New(
						"no GitHub App is connected; run sprun github connect first")
				}
				return err
			}
			if len(installations) == 0 {
				fmt.Fprintln(
					command.OutOrStdout(),
					"The App is connected but installed nowhere yet. "+
						"Install it into an organization from its GitHub settings page.",
				)
				return nil
			}
			for _, installation := range installations {
				fmt.Fprintf(
					command.OutOrStdout(),
					"%d\t%s\t%s\t%s\n",
					installation.ID,
					installation.AccountLogin,
					installation.AccountType,
					installation.RepositorySelection,
				)
			}
			return nil
		},
	}
	command.Flags().StringVar(&stateDirectory, "state-dir", "", "controller state directory (default: OS user config directory)")
	return command
}

// resolveControllerStateDirectory refuses a directory that sprun init has not
// prepared. Writing App credentials into an arbitrary directory would otherwise
// fail deep inside the platform credential store, whose error names neither the
// directory nor the missing step.
func resolveControllerStateDirectory(explicit string) (string, error) {
	directory, err := resolveStateDirectory(explicit, "controller")
	if err != nil {
		return "", err
	}
	if err := app.RequireInitializedControllerState(directory); err != nil {
		return "", fmt.Errorf(
			"%s is not an initialized controller state directory; run sprun init first: %w",
			directory, err,
		)
	}
	return directory, nil
}

// readAppCredential validates the operator-supplied identity and loads the PEM
// from disk. The key bytes are zeroed once the credential owns them so a copy
// does not outlive this call in the process heap.
func readAppCredential(appID int64, clientID, privateKeyFile string) (github.AppCredential, error) {
	if appID <= 0 {
		return github.AppCredential{}, errors.New(
			"--app-id must be the App's numeric ID from its settings page")
	}
	if clientID == "" {
		return github.AppCredential{}, errors.New(
			"--client-id must be the App's client ID from its settings page")
	}
	if privateKeyFile == "" {
		return github.AppCredential{}, errors.New(
			"--private-key-file must be the path to the PEM key downloaded from GitHub")
	}
	file, err := os.Open(privateKeyFile)
	if err != nil {
		return github.AppCredential{}, fmt.Errorf("read private key file: %w", err)
	}
	defer file.Close()
	key, err := io.ReadAll(io.LimitReader(file, maximumAppKeyFileBytes+1))
	if err != nil {
		return github.AppCredential{}, fmt.Errorf("read private key file: %w", err)
	}
	defer clear(key)
	if len(key) > maximumAppKeyFileBytes {
		return github.AppCredential{}, errors.New(
			"private key file is larger than a GitHub App PEM key")
	}
	credential, err := github.NewAppCredential(appID, clientID, string(key))
	if err != nil {
		// The underlying error names the exact validation that failed without
		// echoing any key material.
		return github.AppCredential{}, fmt.Errorf("private key is not a usable App key: %w", err)
	}
	return credential, nil
}
