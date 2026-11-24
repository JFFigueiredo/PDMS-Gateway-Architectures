-- Run as the database owner. Removes only the gateway-owned observation hooks and schema.
BEGIN;
DROP TRIGGER IF EXISTS pdms_observer_outbox ON public.outbox;
DROP TRIGGER IF EXISTS pdms_observer_hashchain ON public.consent_ledger_anchor;
DROP TRIGGER IF EXISTS pdms_observer_besu ON public.onchain_consent_outbox;
DROP VIEW IF EXISTS pdms_observer.queue_state,pdms_observer.workers,pdms_observer.confirmations,pdms_observer.latency,pdms_observer.health;
DROP FUNCTION IF EXISTS pdms_observer.track();
DROP TABLE IF EXISTS pdms_observer.worker_totals,pdms_observer.confirmation_totals,pdms_observer.besu_latency,
 pdms_observer.worker_identity,pdms_observer.installation;
DROP SCHEMA IF EXISTS pdms_observer;
COMMIT;
