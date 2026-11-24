"""Gateway companion: bounded policy/queue gauges and durable observation counters.

Scrapes use cached snapshots. Network/SQL work runs on one background thread, never in PDMS
or the gateway request path. A failed/stale observation becomes unknown, not disabled/empty.
"""
import json
import math
import os
import threading
import time
import urllib.request
from contextlib import closing

import psycopg2
from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily

ARCHS=('monolith','microservices','microservices-async','faas','decentralized')
ORDINARY=('PERSON_CREATED','PERSON_UPDATED','PERSON_DELETED','PERSON_SNAPSHOT_CREATED')
ANCHORS=('CONSENT_CREATED','CONSENT_UPDATED','CONSENT_WITHDRAWN','CONSENT_DELETED')
LEDGER='CONSENT_LEDGER_ANCHOR'
BOUNDS=(0.1,0.5,1,2,5,10,30,60,300,3600)

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        return None

class Observer:
    def __init__(self):
        self.lock=threading.Lock()
        self.states={source:{'ok':False,'at':0,'duration':0,'data':None} for source in ('policy','database')}
        self.base=os.environ['PDMS_OBSERVER_GATEWAY_URL'].rstrip('/')
        self.token=os.environ['PDMS_GATEWAY_ADMIN_TOKEN']
        self.interval=float(os.environ.get('PDMS_OBSERVER_INTERVAL_SECONDS','5'))
        self.max_age=float(os.environ.get('PDMS_OBSERVER_MAX_AGE_SECONDS','20'))
        if self.interval<1 or self.max_age<=self.interval:
            raise ValueError('Invalid observer interval/freshness configuration')
        self.http=urllib.request.build_opener(NoRedirect())

    def get(self,path):
        req=urllib.request.Request(self.base+path,headers={'Authorization':'Bearer '+self.token})
        with self.http.open(req,timeout=3) as response:
            body=response.read(65537)
            if len(body)>65536:
                raise ValueError('Oversized control response')
            return json.loads(body)

    def policy(self):
        status=self.get('/admin/runtime')
        snapshot=status['snapshot']
        version=snapshot['version']
        if (type(version) is not int or version<0
            or status.get('observedCoherent') is not True):
            raise ValueError('Policy observations are not coherent')
        ledger=next(process for process in status['processes'].values() if process['architecture']=='decentralized')
        p=snapshot['policy']
        for flag in ('enabled','ledgerProcessingEnabled','onchainProcessingEnabled','decentralizedOrdinaryProcessingEnabled'):
            if type(p[flag]) is not bool:
                raise ValueError('Invalid policy flag')
        if p['activeArchitecture'] not in (*ARCHS,None) or (p['enabled'] and p['activeArchitecture'] is None):
            raise ValueError('Invalid owner')
        for a in ARCHS:
            types=p['allowedEventTypes'][a]
            if not isinstance(types,list) or not set(types)<=set((*ORDINARY,LEDGER) if a=='decentralized' else ORDINARY):
                raise ValueError('Invalid event allowlist')
        if ledger['architecture']!='decentralized' or type(ledger['blockchainAvailable']) is not bool:
            raise ValueError('Invalid decentralized capability')
        if not isinstance(ledger['effectiveEventTypes'],list) or not isinstance(ledger['effectiveOnchainAnchorTypes'],list):
            raise ValueError('Invalid decentralized permissions')
        if not set(ledger['effectiveEventTypes'])<=set((*ORDINARY,LEDGER)) or not set(ledger['effectiveOnchainAnchorTypes'])<=set(ANCHORS):
            raise ValueError('Unknown permission types')
        return {'version':version,'policy':p,'ledger':ledger}

    def database(self):
        with closing(psycopg2.connect(connect_timeout=3,application_name='pdms-gateway-observer',
            options='-c statement_timeout=3000 -c lock_timeout=1000 -c default_transaction_read_only=on')) as conn:
            conn.set_session(readonly=True,isolation_level='REPEATABLE READ')
            with conn.cursor() as cursor:
                data={}
                for name,query in {
                    'queues':'SELECT queue,event_type,status,events,oldest_timestamp FROM pdms_observer.queue_state',
                    'workers':'SELECT architecture,lane,event_type,result,total FROM pdms_observer.workers',
                    'confirmations':'SELECT ledger,anchor_type,total FROM pdms_observer.confirmations',
                    'latency':'SELECT anchor_type,count,sum_seconds,buckets FROM pdms_observer.latency',
                    'health':'SELECT started_at,errors,tracking_enabled FROM pdms_observer.health',
                }.items():
                    cursor.execute(query)
                    data[name]=cursor.fetchall()
                if len(data['health'])!=1:
                    raise ValueError('Missing observer installation metadata')
                return data

    def poll(self,source,call):
        start=time.monotonic()
        try:
            data=call()
            with self.lock:
                self.states[source]={'ok':True,'at':time.time(),'duration':time.monotonic()-start,'data':data}
        except Exception as error:
            with self.lock:
                self.states[source]={**self.states[source],'ok':False,'duration':time.monotonic()-start}
            # Exception classes only: no DSNs, credentials, response bodies or business values.
            print('Observation failed:',source,type(error).__name__,flush=True)

    def run(self):
        while True:
            self.poll('policy',self.policy)
            self.poll('database',self.database)
            time.sleep(self.interval)

    def collect(self):
        with self.lock:
            states={k:dict(v) for k,v in self.states.items()}
        now=time.time()
        fresh=lambda s:states[s]['ok'] and now-states[s]['at']<=self.max_age
        up=GaugeMetricFamily('pdms_observer_source_up','1 for a successful fresh observation',labels=['source'])
        at=GaugeMetricFamily('pdms_observer_last_success_timestamp_seconds','Last successful observation; zero before first success',labels=['source'])
        duration=GaugeMetricFamily('pdms_observer_collection_duration_seconds','Last collection duration',labels=['source'])
        for source in states:
            up.add_metric([source],int(fresh(source)))
            at.add_metric([source],states[source]['at'])
            duration.add_metric([source],states[source]['duration'])
        yield up; yield at; yield duration
        valid=fresh('policy')
        p=states['policy']['data']['policy'] if valid else None
        ledger=states['policy']['data']['ledger'] if valid else None
        version=GaugeMetricFamily('pdms_outbox_policy_version','Observed policy version')
        version.add_metric([],states['policy']['data']['version'] if valid else math.nan)
        yield version
        owner=GaugeMetricFamily('pdms_outbox_owner','Configured ordinary owner; all zero when no owner is assigned',labels=['architecture'])
        permissions=GaugeMetricFamily('pdms_outbox_processing_permitted','Observed effective lane permission, not completed work',labels=['architecture','lane'])
        events=GaugeMetricFamily('pdms_outbox_event_permitted','Observed effective ordinary event permission',labels=['architecture','event_type'])
        for architecture in ARCHS:
            owner.add_metric([architecture],int(p['activeArchitecture']==architecture) if valid else math.nan)
            ordinary=(p['enabled'] and p['activeArchitecture']==architecture and
                      (architecture!='decentralized' or p['decentralizedOrdinaryProcessingEnabled'])) if valid else None
            for event in ORDINARY:
                events.add_metric([architecture,event],int(ordinary and event in p['allowedEventTypes'][architecture]) if valid else math.nan)
            permissions.add_metric([architecture,'ordinary'],int(ordinary and bool(set(p['allowedEventTypes'][architecture])&set(ORDINARY))) if valid else math.nan)
        for lane,allowed in (('hashchain',LEDGER in ledger['effectiveEventTypes'] if valid else None),
                             ('onchain',bool(ledger['effectiveOnchainAnchorTypes']) if valid else None)):
            permissions.add_metric(['decentralized',lane],int(allowed) if valid else math.nan)
        yield owner; yield permissions; yield events
        flags=GaugeMetricFamily('pdms_outbox_policy_flag','Configured processing flags; effective permissions are separate',labels=['flag'])
        for flag in ('enabled','ledgerProcessingEnabled','onchainProcessingEnabled','decentralizedOrdinaryProcessingEnabled'):
            flags.add_metric([flag],int(p[flag]) if valid else math.nan)
        yield flags
        dbvalid=fresh('database')
        db=states['database']['data'] if dbvalid else None
        queues={(q,e,s):(n,float(ts)) for q,e,s,n,ts in db['queues']} if dbvalid else {}
        depth=GaugeMetricFamily('pdms_outbox_queue_events','Shared queue row counts; export once per database',labels=['queue','event_type','status'])
        oldest=GaugeMetricFamily('pdms_outbox_oldest_pending_timestamp_seconds','Oldest pending timestamp; zero for empty queue',labels=['queue','event_type'])
        age=GaugeMetricFamily('pdms_outbox_oldest_pending_age_seconds','Age of oldest pending row; zero for empty queue',labels=['queue','event_type'])
        for queue,types,statuses in (('shared',(*ORDINARY,LEDGER,'unknown'),('PENDING','PROCESSING','FAILED')),
                                     ('onchain',(*ANCHORS,'unknown'),('PENDING','CONFIRMED','FAILED'))):
            for event in types:
                for status in statuses:
                    depth.add_metric([queue,event,status],queues.get((queue,event,status),(0,0))[0] if dbvalid else math.nan)
                timestamp=queues.get((queue,event,'PENDING'),(0,0))[1]
                oldest.add_metric([queue,event],timestamp if dbvalid else math.nan)
                age.add_metric([queue,event],max(0,now-timestamp) if dbvalid and timestamp else (0 if dbvalid else math.nan))
        yield depth; yield oldest; yield age
        tracking=GaugeMetricFamily('pdms_observer_tracking_healthy','Hooks enabled and no bookkeeping errors observed since installation')
        tracking.add_metric([],int(db['health'][0][2] and db['health'][0][1]==0) if dbvalid else math.nan)
        yield tracking
        if not dbvalid:
            return  # Durable counters become absent rather than falsely resetting to zero.
        errors=CounterMetricFamily('pdms_observer_tracking_errors','Bookkeeping errors, committed with the business transaction')
        errors.add_metric([],db['health'][0][1]); yield errors
        started=GaugeMetricFamily('pdms_observer_tracking_start_timestamp_seconds','Bookkeeping starts here; no historical backfill')
        started.add_metric([],db['health'][0][0].timestamp()); yield started
        workers=CounterMetricFamily('pdms_outbox_events','Committed observed worker state transitions, not unique deliveries',labels=['architecture','lane','event_type','result'])
        for architecture,lane,event,result,total in db['workers']:
            # Never turn arbitrary database contents into unbounded metric labels.
            if architecture in (*ARCHS,'unknown') and lane in ('ordinary','hashchain','onchain') and event in (*ORDINARY,*ANCHORS,LEDGER,'unknown') and result in ('completed','failed','retry','reset'):
                workers.add_metric([architecture,lane,event,result],total)
        yield workers
        confirmations=CounterMetricFamily('pdms_ledger_confirmations','Committed anchors/confirmations observed since installation',labels=['ledger','anchor_type'])
        for kind,anchor,total in db['confirmations']:
            if kind in ('hashchain','besu') and anchor in (*ANCHORS,'unknown'):
                confirmations.add_metric([kind,anchor],total)
        yield confirmations
        latency=HistogramMetricFamily('pdms_ledger_confirmation_seconds','On-chain enqueue to committed CONFIRMED observation, including pauses',labels=['ledger','anchor_type'])
        for anchor,count,total,buckets in db['latency']:
            if anchor in (*ANCHORS,'unknown'):
                latency.add_metric(['besu',anchor],[(str(bound),value) for bound,value in zip(BOUNDS,buckets)]+[('+Inf',count)],sum_value=total)
        yield latency

if __name__=='__main__':
    observer=Observer()
    registry=CollectorRegistry()
    registry.register(observer)
    start_http_server(9400,registry=registry)
    observer.run()
