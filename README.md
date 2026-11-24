# PDMS Gateway Architectures

Reproduce the Gateway conditions for Experiment 2 (T1–T5) with the [case inputs](reproduce/cases) and [pinned source revisions](reproduce/source-revisions.json).

| File | Use |
|---|---|
| [Deployment guide](DEPLOYMENT_GUIDE.md) | Start the shared services, Gateway, backends and monitoring |
| [Besu Compose](docker-compose.besu.yml) | Start the private development ledger for the decentralized backend |
| [Outbox runtime control](OUTBOX_RUNTIME_CONTROL.md) | Configure and verify runtime ownership and worker handover |
| [LICENSE](LICENSE) | Apache License 2.0 |

## Requirements

- Java 25 and the included Maven wrapper for building from source.
- Docker with Compose for deployment.
- [PDMS Architectures v2.0.0](https://github.com/JFFigueiredo/PDMS_Architectures/tree/v2.0.0) and its dependencies.
- The original mixed workload driver and request plan for exact replay. The case files provide schedules and seeds. Supply provisioning and capture tooling for your environment.

## Build

Run `sh ./mvnw verify` on Linux or macOS, or `.\mvnw.cmd verify` on Windows.

## Prepare an experiment arm

Prepare local settings and services using the [deployment guide](DEPLOYMENT_GUIDE.md). Restore the same baseline for both arms and use a fresh Gateway process and run ID for each:

   ```sh
   python reproduce/configure.py T1 1 reference t1-p01-reference
   docker compose --env-file .env --env-file config/generated/t1-p01-reference/experiment.env up -d --build
   ```

Verify [runtime readiness](OUTBOX_RUNTIME_CONTROL.md), then run the workload using the generated `workload.json`. Follow the captured pair order. Collect client, Gateway and monitoring records through drain. Stop the Gateway gracefully and archive its request journal before removing the log volume.

The copy includes the observer file-permission fix and the Gateway test timing fix from its source snapshot. The original deployed Gateway revision is recorded separately. To reproduce the original image exactly, use the recorded revision and the original image provenance.
