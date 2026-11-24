# Gateway runtime and outbox control

The Gateway coordinates the active architecture for business HTTP and ordinary outbox work. Each backend process has an in-memory flag. A split architecture has three participants, and the Gateway coordinates all three. Keep one Gateway writer for a deployment, and route business traffic through it.

## Configure and check readiness

- Set `PDMS_GATEWAY_RUNTIME_MANAGED=true` and use one shared `PDMS_OUTBOX_ADMIN_TOKEN` for the Gateway and all backends.
- Set a separate `PDMS_GATEWAY_ADMIN_TOKEN` for Gateway admin calls.
- Keep the backend health and authenticated management endpoints reachable by the Gateway. Backend management uses `/actuator/outbox-control`. The Gateway's [runtime client](src/main/java/ipl/pdms/gateway/runtime/RuntimeControlClient.java) owns those requests during a run.
- Check the Gateway process at `GET /gateway/actuator/health`. Use authenticated `GET /admin/health` and `GET /admin/runtime` before sending load. Require every configured architecture to report `UP`, runtime phase `ready`, `observedCoherent=true`, `businessAdmissionOpen=true`, and the intended starting owner in the runtime policy.

## During a handover

The [runtime coordinator](src/main/java/ipl/pdms/gateway/runtime/RuntimeCoordinator.java) closes admission and drains in-flight Gateway requests. It disables business HTTP and waits for ordinary work to drain across all configured backend processes. It then switches routing, enables the new owner, and checks every process's acknowledgement before reopening admission. Preserve the selection, runtime and request journals so the decision, applied handover and request impact can be reconciled.

Pending events stay in the database outbox. The ordinary runtime flag leaves the decentralized ledger lane independent. Besu connectivity and signing are configured at backend startup, so inspect ledger confirmations and pending work separately. A declared capability or successful handover alone does not establish onchain confirmation.

## Restart and failure

The Gateway starts unreconciled and needs fresh observations from all configured backend processes. Backends also have in-memory state that can change on restart. Check runtime coherence and admission after a restart or uncertain management timeout. Avoid direct backend enable or disable calls while the Gateway owns the deployment. Preserve evidence and stop experimental traffic if the owner, routing or acknowledgements disagree.

See [the backend runtime contract](https://github.com/JFFigueiredo/PDMS_Architectures/blob/v2.0.0/OUTBOX_RUNTIME_CONTROL.md) for the backend flag, acknowledgement and ledger behavior.
