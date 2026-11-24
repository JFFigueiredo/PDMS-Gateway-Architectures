# Gateway deployment guide

## 1. Prepare the backends and private configuration

Check out [PDMS Architectures v2.0.0](https://github.com/JFFigueiredo/PDMS_Architectures/tree/v2.0.0). Its [deployment guide](https://github.com/JFFigueiredo/PDMS_Architectures/blob/v2.0.0/DEPLOYMENT_GUIDE.md) lists the Compose files for PostgreSQL/Kafka and the five application variants. Place the Gateway, database, broker, app processes and ledger on addresses reachable over your experiment network.

Copy `.env.example` to `.env` and `config/routing.example.json` to `config/routing.json`. Put all app addresses in the routing file, including each person, snapshot and consent endpoint for both split architectures. Populate the Flyway URL, user and password in the local environment file. Generate a new Gateway admin token and a distinct backend outbox token. Use the same outbox token on the Gateway and all nine backend processes. Configure backend JWT verification and the workload driver's signing key for your deployment.

## 2. Start PostgreSQL, Kafka and Besu

Start PostgreSQL and Kafka using the backend checkout's `docker-compose_DB_KAFKA.yml`. Use the Gateway as the Flyway migration owner for the shared database. Set `PDMS_FLYWAY_ENABLED=false` on all backends and keep Gateway Flyway enabled.

For the decentralized variant, generate a fresh ledger signing key privately and derive its public address. Configure these environment variables locally for [the Besu Compose file](docker-compose.besu.yml):

| Variable | Value |
|---|---|
| `PDMS_ARCHITECTURES_DIR` | Path to the tagged backend checkout, with its `docker/besu` build context |
| `BESU_SIGNER_ADDRESS` | The public address of your fresh signing key, used as the mining beneficiary |
| `BESU_BIND` | Host bind address for RPC and metrics. Set it to loopback for local use |
| `BESU_LISTEN_ADDRESS` | Container listen address. Set it to the wildcard address so Docker can forward published traffic |
| `BESU_HOST_ALLOWLIST` | The RPC host names or addresses you use, including the private host address for a remote backend |

```sh
docker compose -f docker-compose.besu.yml up -d --build
```

This starts a private development chain. Keep its RPC and metrics on the experiment network. If Besu cannot write to the data volume, set that volume's ownership to the UID and GID of the Besu image's configured user, then restart. Check that `eth_chainId` returns `0x539`, the chain height increases, the configured signer receives a balance, and the metrics endpoint responds. Preserve the chain baseline and signer setup for paired resets.

For the decentralized app, use a local Compose override for `PDMS_BLOCKCHAIN_RPC_URL`, `PDMS_BLOCKCHAIN_PRIVATE_KEY`, `PDMS_BLOCKCHAIN_ENABLED=true`, and `PDMS_BLOCKCHAIN_CHAIN_ID=1337`. Supply the fresh private key through your local secret configuration. The signer in the app must match the funded public address used by Besu. Check that the effective environment overrides the values in the backend Compose file before starting the app, and keep any rendered configuration containing credentials private.

## 3. Start the Gateway and the five backends

Start the Gateway with the prepared routing and database settings. Wait until its Flyway migrations succeed, then start all five backends with their application Compose files. Leave runtime management enabled. The Gateway reconciles the starting owner once the configured backend processes are healthy. See [Outbox runtime control](OUTBOX_RUNTIME_CONTROL.md) for the readiness criteria and failure handling.

Set `PDMS_GATEWAY_BIND` in your local environment. Use loopback for local workloads and the experiment interface for a remote driver. Supply any certificates or trust material required by your deployment separately.

For ledger, queue and policy metrics, install [the observer schema](observability/install_observer.sql) and set `PDMS_OBSERVER_BIND`, database credentials and network in `observability/.env`. With the Gateway and database reachable from that network, run:

```sh
docker compose --env-file observability/.env -f observability/compose.yml up -d --build
```

Set `PDMS_NETWORK` and `PDMS_MONITORING_BIND` locally before starting [Prometheus and Grafana](observability/local/compose.yml). Replace the [scrape hostnames](observability/local/prometheus.yml) with names reachable from Prometheus. Preserve the `architecture` and `process` labels for the dashboards.

## 4. Run and capture a matched arm

Follow the [arm commands](README.md#prepare-an-experiment-arm). Keep the generated workload and Gateway inputs with the capture. Restore business tables, Kafka state and chain state where applicable before the next arm. Use fresh logs and an isolated metrics store for each arm.
