-- Optional gateway-owned bookkeeping, installed by the database owner, outside Flyway.
-- No business payloads or identifiers are retained. Counts begin at installation.
BEGIN;
CREATE SCHEMA IF NOT EXISTS pdms_observer;
REVOKE CREATE ON SCHEMA pdms_observer FROM PUBLIC;
CREATE TABLE IF NOT EXISTS pdms_observer.installation (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(), errors bigint NOT NULL DEFAULT 0);
INSERT INTO pdms_observer.installation(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS pdms_observer.worker_identity (
  application_name text PRIMARY KEY, architecture text NOT NULL CHECK(architecture IN
    ('monolith','microservices','microservices-async','faas','decentralized')));
INSERT INTO pdms_observer.worker_identity VALUES
 ('smoke-monolith','monolith'),('pdms-monolith','monolith'),
 ('smoke-faas','faas'),('pdms-faas','faas'),
 ('smoke-decentralized','decentralized'),('pdms-hybrid-decentralized','decentralized'),
 ('smoke-ms-person','microservices'),('smoke-ms-consent','microservices'),('smoke-ms-snapshot','microservices'),
 ('pdms-microservice-person','microservices'),('pdms-microservice-consent','microservices'),('pdms-microservice-snapshot','microservices'),
 ('smoke-msa-person','microservices-async'),('smoke-msa-consent','microservices-async'),('smoke-msa-snapshot','microservices-async'),
 ('pdms-microservice-person-async','microservices-async'),('pdms-microservice-consent-async','microservices-async'),('pdms-microservice-snapshot-async','microservices-async')
 ON CONFLICT(application_name) DO UPDATE SET architecture=EXCLUDED.architecture;
CREATE TABLE IF NOT EXISTS pdms_observer.worker_totals (
  architecture text NOT NULL, lane text NOT NULL, event_type text NOT NULL, result text NOT NULL,
  total bigint NOT NULL DEFAULT 0, PRIMARY KEY(architecture,lane,event_type,result));
CREATE TABLE IF NOT EXISTS pdms_observer.confirmation_totals (
  ledger text NOT NULL, anchor_type text NOT NULL, total bigint NOT NULL DEFAULT 0,
  PRIMARY KEY(ledger,anchor_type));
CREATE TABLE IF NOT EXISTS pdms_observer.besu_latency (
  anchor_type text PRIMARY KEY, count bigint NOT NULL DEFAULT 0, sum_seconds double precision NOT NULL DEFAULT 0,
  buckets bigint[] NOT NULL DEFAULT ARRAY[0,0,0,0,0,0,0,0,0,0]::bigint[]);
INSERT INTO pdms_observer.confirmation_totals(ledger,anchor_type)
 SELECT ledger,anchor_type FROM unnest(ARRAY['hashchain','besu']) ledger
 CROSS JOIN unnest(ARRAY['CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED','unknown']) anchor_type
 ON CONFLICT DO NOTHING;
INSERT INTO pdms_observer.besu_latency(anchor_type)
 SELECT unnest(ARRAY['CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED','unknown'])
 ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION pdms_observer.track() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
 SET search_path=pg_catalog,pdms_observer AS $$
DECLARE arch text; lane_name text; kind text; outcome text; anchor text;
  delta bigint := 1; seconds double precision; increments bigint[];
BEGIN
  SELECT architecture INTO arch FROM pdms_observer.worker_identity
    WHERE application_name=current_setting('application_name');
  arch:=coalesce(arch,'unknown');
  IF TG_TABLE_NAME='outbox' THEN
    kind:=CASE WHEN OLD.event_type IN ('PERSON_CREATED','PERSON_UPDATED','PERSON_DELETED',
      'PERSON_SNAPSHOT_CREATED','CONSENT_LEDGER_ANCHOR') THEN OLD.event_type ELSE 'unknown' END;
    lane_name:=CASE WHEN kind='CONSENT_LEDGER_ANCHOR' THEN 'hashchain' ELSE 'ordinary' END;
    IF TG_OP='DELETE' AND OLD.processing_status='COMPLETED' THEN outcome:='completed';
    ELSIF TG_OP='UPDATE' THEN
      IF NEW.processing_status='FAILED' AND OLD.processing_status<>'FAILED' THEN outcome:='failed';
      ELSIF NEW.processing_status='PENDING' AND OLD.processing_status='FAILED' THEN outcome:='retry';
      ELSIF NEW.processing_status='PENDING' AND OLD.processing_status='PROCESSING' THEN outcome:='reset';
      END IF;
    END IF;
  ELSIF TG_TABLE_NAME='consent_ledger_anchor' THEN
    anchor:=CASE WHEN NEW.anchor_type IN ('CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED')
      THEN NEW.anchor_type ELSE 'unknown' END;
    UPDATE pdms_observer.confirmation_totals SET total=total+1 WHERE ledger='hashchain' AND anchor_type=anchor;
  ELSIF TG_TABLE_NAME='onchain_consent_outbox' THEN
    anchor:=CASE WHEN NEW.anchor_type IN ('CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED')
      THEN NEW.anchor_type ELSE 'unknown' END;
    lane_name:='onchain'; kind:=anchor;
    IF NEW.status='CONFIRMED' AND OLD.status<>'CONFIRMED' THEN
      outcome:='completed';
      UPDATE pdms_observer.confirmation_totals SET total=total+1 WHERE ledger='besu' AND anchor_type=anchor;
      seconds:=greatest(0,extract(epoch FROM clock_timestamp()-NEW.created_at));
      SELECT array_agg((seconds<=b)::int::bigint ORDER BY ord) INTO increments
        FROM unnest(ARRAY[0.1,0.5,1,2,5,10,30,60,300,3600]::double precision[]) WITH ORDINALITY t(b,ord);
      UPDATE pdms_observer.besu_latency SET count=count+1,sum_seconds=sum_seconds+seconds,
        buckets=ARRAY(SELECT buckets[i]+increments[i] FROM generate_series(1,10) i) WHERE anchor_type=anchor;
    ELSIF NEW.status='FAILED' AND OLD.status<>'FAILED' THEN outcome:='failed';
    ELSIF NEW.retry_count>OLD.retry_count THEN outcome:='retry'; delta:=NEW.retry_count-OLD.retry_count;
    END IF;
  END IF;
  IF outcome IS NOT NULL THEN
    INSERT INTO pdms_observer.worker_totals VALUES(arch,lane_name,kind,outcome,delta)
      ON CONFLICT(architecture,lane,event_type,result) DO UPDATE
      SET total=pdms_observer.worker_totals.total+EXCLUDED.total;
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
EXCEPTION WHEN OTHERS THEN
  -- Metrics must not abort the business mutation. A nonzero error count invalidates coverage.
  BEGIN UPDATE pdms_observer.installation SET errors=errors+1 WHERE singleton;
  EXCEPTION WHEN OTHERS THEN RAISE WARNING 'PDMS observer unavailable (SQLSTATE %)',SQLSTATE;
  END;
  IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
REVOKE ALL ON FUNCTION pdms_observer.track() FROM PUBLIC;
DROP TRIGGER IF EXISTS pdms_observer_outbox ON public.outbox;
CREATE TRIGGER pdms_observer_outbox AFTER UPDATE OR DELETE ON public.outbox
 FOR EACH ROW EXECUTE FUNCTION pdms_observer.track();
DROP TRIGGER IF EXISTS pdms_observer_hashchain ON public.consent_ledger_anchor;
CREATE TRIGGER pdms_observer_hashchain AFTER INSERT ON public.consent_ledger_anchor
 FOR EACH ROW EXECUTE FUNCTION pdms_observer.track();
DROP TRIGGER IF EXISTS pdms_observer_besu ON public.onchain_consent_outbox;
CREATE TRIGGER pdms_observer_besu AFTER UPDATE ON public.onchain_consent_outbox
 FOR EACH ROW EXECUTE FUNCTION pdms_observer.track();

CREATE OR REPLACE VIEW pdms_observer.queue_state AS
 SELECT 'shared'::text AS queue,
 CASE WHEN event_type IN ('PERSON_CREATED','PERSON_UPDATED','PERSON_DELETED','PERSON_SNAPSHOT_CREATED','CONSENT_LEDGER_ANCHOR')
   THEN event_type ELSE 'unknown' END AS event_type,processing_status AS status,count(*)::bigint AS events,
 extract(epoch FROM min(created_at) AT TIME ZONE current_setting('TimeZone'))::double precision AS oldest_timestamp
 FROM public.outbox GROUP BY 1,2,3
 UNION ALL
 SELECT 'onchain',CASE WHEN anchor_type IN ('CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED')
   THEN anchor_type ELSE 'unknown' END,status,count(*),extract(epoch FROM min(created_at))::double precision
 FROM public.onchain_consent_outbox GROUP BY 1,2,3;
CREATE OR REPLACE VIEW pdms_observer.workers AS SELECT * FROM pdms_observer.worker_totals;
CREATE OR REPLACE VIEW pdms_observer.confirmations AS SELECT * FROM pdms_observer.confirmation_totals;
CREATE OR REPLACE VIEW pdms_observer.latency AS SELECT * FROM pdms_observer.besu_latency;
CREATE OR REPLACE VIEW pdms_observer.health AS SELECT started_at,errors,
 (SELECT count(*)=3 FROM pg_trigger WHERE tgname IN
   ('pdms_observer_outbox','pdms_observer_hashchain','pdms_observer_besu') AND tgenabled IN ('O','A')) AS tracking_enabled
 FROM pdms_observer.installation;
REVOKE ALL ON ALL TABLES IN SCHEMA pdms_observer FROM PUBLIC;
COMMIT;
