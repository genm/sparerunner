# SpareRunner for Raycast

Control this computer's SpareRunner node from Raycast: see what it is running, and
stop or resume accepting new fleet jobs.

The extension is a client of the `sprun` CLI, not of the agent socket. It holds
no controller credential and no fleet address, so it can only affect the computer
it runs on. Fleet-wide control stays in the Web UI and the CLI.

## Requirements

- The SpareRunner agent running with its local control endpoint:
  `sparerunner-agent serve --local-control`
- The `sprun` CLI installed. The extension searches the standard locations and
  accepts an explicit path in its preferences.

## Commands

| Command | Effect |
|---|---|
| Node Status | Show acceptance, controller connection, and running executions |
| Stop Accepting Jobs | Withhold this computer's capacity; a running job finishes normally |
| Resume Accepting Jobs | Offer capacity again once the controller confirms it |

Stopping never cancels a job that is already running. Resuming reports `pending`
until the controller acknowledges it, and a missing CLI or unreachable agent is
shown as an explicit error rather than an assumed state.

## Development

```bash
npm install
npm run dev
```
