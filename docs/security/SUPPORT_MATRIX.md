# Support matrix

This matrix describes the intended first tagged release. A row is supported
only after its real host evidence is attached to the release gate; a successful
cross-build is not a substitute for service or runner evidence.

| Surface | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Controller binary | supported target | supported target | supported target |
| Agent binary | supported target | supported target | supported target |
| Native ephemeral runner | systemd + service account | launchd + Keychain | Windows Service + DPAPI/Job Object |
| Generic `sparerunner` profile | live evidence required | live evidence required | live evidence required |
| OS-specific profile | `sparerunner-linux` | `sparerunner-macos` | `sparerunner-windows` |
| Automatic restart/reconnect | live evidence required | reboot/sleep evidence required | reboot/recovery evidence required |
| Credential storage | service-user credential file/systemd credential | Keychain | DPAPI/CNG |
| Docker runner | not in first release | not in first release | not in first release |
| WAN/Iroh transport | not in first release | not in first release | not in first release |

The first release gate requires at least one real node for each OS and two
independent GitHub App installations. `GOOS` cross-build output, a mock Agent,
or a local fake GitHub provider can validate code paths but cannot mark the
corresponding matrix row as live-supported.
