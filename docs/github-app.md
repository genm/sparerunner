# Connecting a GitHub App

Tewake acts as a GitHub App that **you own**. This page is the primary setup
path: it needs no browser session with the controller and no management UI.

## Why the App is yours

A GitHub App mints installation tokens by signing a JWT with the App's private
key, so whatever acts as the App must hold that key. Tewake keeps the key on
your controller host, in the platform credential store, and never sends it
anywhere.

That is also why there is no shared "Tewake App" to install. One shared App
would mean one shared private key distributed to every controller, and whoever
held it could mint tokens for every other installation of that App — every other
user's organizations. A hosted service could hold the key instead, but that
would contradict Tewake's LAN-first boundary: no cloud dependency, credentials
never leave the machine.

## 1. Create the App

Open <https://github.com/settings/apps/new> for a personal App, or
`https://github.com/organizations/<org>/settings/apps/new` to have the
organization own it. Either works; ownership only decides who administers it.

Set:

- **GitHub App name**: anything unique, for example `tewake-<your-fleet>`
- **Homepage URL**: anything, for example your repository URL
- **Webhook**: **uncheck Active**. Tewake polls scale sets and needs no webhook
  delivery.
- **Repository permissions**
  - Administration: **Read-only**
  - Metadata: **Read-only** (GitHub selects this automatically)
- **Organization permissions**
  - Self-hosted runners: **Read and write**
- **Actions** (repository permission): **Read and write**

Those four permissions are exactly what the controller uses: it reads scope
metadata to verify a Target is private, manages the runner group and scale set
it owns, and drives the Actions runner protocol. Nothing here grants access to
your code contents.

After creating the App:

1. Note the **App ID** and **Client ID** from its settings page.
2. **Generate a private key** and keep the downloaded `.pem` file for the next
   step.

## 2. Connect it to the controller

```bash
tewake github connect \
  --app-id 1234567 \
  --client-id Iv1.0123456789abcdef \
  --private-key-file ~/Downloads/your-app.private-key.pem
```

The key is read from the file and handed to this host's credential store —
Keychain on macOS, DPAPI on Windows, a service-user-only file on Linux. It is
never accepted as a flag value, because a command-line argument is visible to
every process on the host and lands in shell history. Delete the `.pem` once the
command succeeds.

Connecting is idempotent for the same App. Connecting a *different* App is
refused: rebinding a controller would strand every Target already provisioned
through the first App.

## 3. Install the App into your accounts

From the App's settings page, choose **Install App** and install it into each
organization or user account whose private repositories should reach this
fleet. To install into an organization you do not personally own the App under,
make the App public first (Advanced → *Make this GitHub App public*); this
allows installation elsewhere and does not publish your key or code.

Then confirm what the controller can see:

```bash
tewake github installations
# 149442642   your-org   Organization   all
```

The first column is the installation ID a Target refers to.

## 4. Create a Target

Targets are ordinary configuration, so the same CLI completes the setup:

```bash
tewake config export > fleet.yaml
```

Add a runner profile and a Target, keeping the installation ID from above:

```yaml
runnerProfiles:
  - id: profile-tewake
    label: tewake
    minAvailableMemoryBytes: 0
    versionPolicy: auto_update
    runtime: native
targets:
  - id: target-your-org
    installationId: "149442642"
    scopeKind: organization
    scope: your-org
    scaleSetName: tewake
    runnerProfileId: profile-tewake
```

```bash
tewake config apply fleet.yaml
```

Applying verifies the scope against GitHub before anything is committed: a
public scope, an unverifiable visibility, or unsafe runner-group access is
rejected, and the runner group and scale set Tewake owns are created only after
that check passes.

## The Web UI path

The loopback console offers the same connection through GitHub's App Manifest
flow, which creates the App and hands its key back in one confirmation instead
of the manual steps above. It is a convenience, not a requirement — everything
on this page works without ever opening it.
