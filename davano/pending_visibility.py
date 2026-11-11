"""Bounded, GET-only Kueue visibility. Never treats an owner UID as Workload UID."""
import os
import re
import time
from datetime import datetime, timezone

from .kube import ApiError

GROUP = '/apis/visibility.kueue.x-k8s.io'
LIMIT = 100
MAX_TARGETS = 10
BUDGET_SECONDS = 12
_LABEL = re.compile(r'[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z')


def _valid_name(value):
    return isinstance(value, str) and len(value) <= 253 and all(_LABEL.fullmatch(label) for label in value.split('.'))


def timestamp():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def unknown_position(reason='No UID-verified pending position is available.'):
    return {'status': 'unknown', 'reason': reason, 'local_queue_position': None,
            'cluster_queue_position': None, 'observed_at': None,
            'source': 'kueue-visibility', 'position_base': 0}


def empty_visibility(status='disabled', detail='Pending visibility is separately opt-in; no admission time is predicted.'):
    return {'status': status, 'detail': detail, 'api_version': None, 'observed_at': None,
            'targets': [], 'limit': LIMIT, 'offset': 0}


def parse_targets(raw):
    if not isinstance(raw, str) or len(raw) > 4000:
        raise ValueError('Invalid targets')
    entries = raw.split(',')
    if not 1 <= len(entries) <= MAX_TARGETS:
        raise ValueError('Invalid target count')
    targets = []
    for entry in entries:
        parts = entry.strip().split('/')
        if len(parts) != 2 or not all(_valid_name(p) for p in parts) or len(parts[0]) > 63 or '.' in parts[0]:
            raise ValueError('Invalid LocalQueue target')
        target = tuple(parts)
        if target in targets:
            raise ValueError('Duplicate target')
        targets.append(target)
    return targets


def configured_targets():
    if os.getenv('DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED', 'false').lower() != 'true':
        return None
    return parse_targets(os.getenv('DAVANO_KUEUE_PENDING_LOCAL_QUEUES', ''))


def _list(value):
    if not isinstance(value, list):
        raise ValueError('Expected list')
    return value


def _valid_position(value):
    return type(value) is int and 0 <= value <= 2147483647


def _current_pending(workload):
    # Visibility is a separate read: only corroborate current negative admission.
    cs = workload.get('conditions', [])
    for kind in ('Admitted', 'QuotaReserved'):
        found = [c for c in cs if c.get('type') == kind]
        if len(found) != 1 or found[0].get('current_generation') is not True or found[0].get('status') != 'False':
            return False
    return workload.get('finished') is not True


class PendingVisibilityReader:
    def __init__(self, client, targets, clock=time.monotonic):
        # Revalidate injected configuration as well as environment configuration.
        self.targets = parse_targets(','.join('/'.join(t) for t in targets))
        self.client, self.clock = client, clock

    def snapshot(self, workloads):
        output = empty_visibility('connected', 'Bounded queue observations only, not ETA, admission guarantees or GPU ownership. Upstream may omit Workload UID; unmatched positions remain unknown.')
        deadline = self.clock() + BUDGET_SECONDS

        def get(path):
            if self.clock() >= deadline:
                raise ApiError(504, 'Visibility budget exhausted')
            value = self.client.get(path)
            if self.clock() >= deadline:
                raise ApiError(504, 'Visibility budget exhausted')
            if not isinstance(value, dict):
                raise ValueError('Invalid visibility object')
            return value

        try:
            group = get(GROUP)
            versions = {v.get('version') for v in _list(group.get('versions')) if isinstance(v, dict)}
            version = next((v for v in ('v1beta2', 'v1beta1') if v in versions), None)
            if version is None:
                return empty_visibility('unavailable', 'No supported served Kueue visibility API was discovered.')
            root = GROUP + '/' + version
            discovery = get(root)
            resources = _list(discovery.get('resources'))
            if not any(isinstance(r, dict) and r.get('name') == 'localqueues/pendingworkloads' and r.get('namespaced') is True and 'get' in r.get('verbs', []) for r in resources):
                return empty_visibility('unavailable', 'Discovery does not advertise namespaced pending-workload GET access.')
            output['api_version'] = version
        except (ApiError, ValueError, TypeError, AttributeError, KeyError):
            return empty_visibility('unavailable', 'Kueue visibility discovery unavailable; check installation and read-only RBAC.')

        for namespace, name in self.targets:
            target = {'namespace': namespace, 'name': name, 'status': 'unavailable',
                      'detail': 'Pending summary unavailable; check read-only RBAC, response validity and request budget.',
                      'observed_at': None, 'returned_count': None, 'coverage': 'unknown', 'reported_entries': []}
            output['targets'].append(target)
            try:
                summary = get(f'{root}/namespaces/{namespace}/localqueues/{name}/pendingworkloads?limit={LIMIT}&offset=0')
                if summary.get('kind') != 'PendingWorkloadsSummary' or summary.get('apiVersion') != f'visibility.kueue.x-k8s.io/{version}':
                    raise ValueError('Unsupported summary')
                items = _list(summary.get('items'))
                if len(items) > LIMIT or summary.get('metadata', {}).get('continue') or summary.get('warnings'):
                    raise ValueError('Partial or oversized response')
                seen_names, seen_uids, seen_local, seen_cluster = set(), set(), set(), set()
                for index, item in enumerate(items):
                    meta = item['metadata']
                    identity = (meta.get('namespace'), meta.get('name'))
                    local, cluster = item.get('positionInLocalQueue'), item.get('positionInClusterQueue')
                    uid = meta.get('uid')
                    if (identity[0] != namespace or not _valid_name(identity[1])
                            or item.get('localQueueName') != name or not _valid_position(local) or not _valid_position(cluster)
                            or local != index or cluster < local or (seen_cluster and cluster <= max(seen_cluster)) or identity in seen_names or local in seen_local or cluster in seen_cluster
                            or (uid is not None and (not isinstance(uid, str) or not re.fullmatch(r'[A-Za-z0-9-]{1,128}', uid) or uid in seen_uids))):
                        raise ValueError('Conflicting summary')
                    seen_names.add(identity)
                    seen_local.add(local)
                    seen_cluster.add(cluster)
                    if uid:
                        seen_uids.add(uid)
                at = timestamp()
                target['reported_entries'] = [
                    {'namespace': namespace, 'name': item['metadata']['name'],
                     'local_queue_position': item['positionInLocalQueue'],
                     'cluster_queue_position': item['positionInClusterQueue'],
                     'position_base': 0, 'identity_status': 'unverified',
                     'source': 'kueue-visibility-summary', 'observed_at': at}
                    for item in items]
                # A full bounded window might be truncated. Fail closed for joins.
                partial = len(items) == LIMIT
                target.update(status='partial' if partial else 'observed', observed_at=at, returned_count=len(items), coverage='bounded-window',
                              detail='Window reached its limit; Workload-bound positions remain unknown.' if partial else 'Observed bounded window, not a total backlog or start-time prediction. Workload UID may be omitted by upstream.')
                if partial:
                    continue
                for item in items:
                    meta = item['metadata']
                    matches = [w for w in workloads if w.get('namespace') == namespace and w.get('name') == meta['name']]
                    uid = meta.get('uid')
                    if not uid:
                        for w in matches:
                            w['pending_position'] = unknown_position('The visibility summary omits Workload UID; name or owner UID cannot verify identity.')
                        continue
                    uid_matches = [w for w in workloads if w.get('uid') == uid]
                    if len(matches) != 1 or len(uid_matches) != 1:
                        continue
                    w = matches[0]
                    if w is not uid_matches[0] or w.get('queue') != name or not _current_pending(w):
                        continue
                    w['pending_position'] = {'status': 'observed', 'reason': 'UID-matched, non-atomic queue position observation; not an admission guarantee.',
                                             'local_queue_position': item['positionInLocalQueue'], 'cluster_queue_position': item['positionInClusterQueue'],
                                             'observed_at': at, 'source': 'kueue-visibility', 'position_base': 0}
            except (ApiError, ValueError, TypeError, AttributeError, KeyError):
                pass
        statuses = {t['status'] for t in output['targets']}
        output['status'] = 'unavailable' if statuses == {'unavailable'} else 'partial' if statuses != {'observed'} else 'connected'
        output['observed_at'] = timestamp() if statuses != {'unavailable'} else None
        return output
