#!/usr/bin/env python3
"""Prepare one hash-pinned official Kueue manifest; never contact Kubernetes.

Download is explicit. The original artifact is checked before YAML parsing, and
only the reviewed controller image is changed to its pinned OCI index digest.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.request import Request, urlopen

import yaml

PROFILE = Path(__file__).resolve().parents[1] / 'acceptance' / 'profile.json'
MAX_BYTES = 2 * 1024 * 1024


def load_profile():
    raw = PROFILE.read_bytes()
    if len(raw) > 16 * 1024:
        raise ValueError('Invalid profile')
    profile = json.loads(raw)
    k = profile['kueue']
    if (profile['schema_version'] != 1 or k['version'] != 'v0.20.5'
            or k['api_version'] != 'v1beta2'
            or k['manifest_url'] != 'https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.5/manifests.yaml'
            or type(k['manifest_bytes']) is not int or not 0 < k['manifest_bytes'] <= MAX_BYTES
            or not re.fullmatch('[a-f0-9]{64}', k['manifest_sha256'])
            or k['upstream_image'] != 'registry.k8s.io/kueue/kueue:v0.20.5'
            or not re.fullmatch(re.escape(k['upstream_image']) + r'@sha256:[a-f0-9]{64}', k['image'])
            or k['namespace'] != 'kueue-system' or k['deployment'] != 'kueue-controller-manager'):
        raise ValueError('Invalid profile')
    return profile


def prepare(raw, profile):
    k = profile['kueue']
    if (len(raw) != k['manifest_bytes'] or len(raw) > MAX_BYTES
            or hashlib.sha256(raw).hexdigest() != k['manifest_sha256']):
        raise ValueError('Pinned artifact did not match')
    objects = list(yaml.safe_load_all(raw))
    if not objects or len(objects) > 128 or not all(isinstance(o, dict) for o in objects):
        raise ValueError('Invalid official manifest')
    deployments = [o for o in objects if o.get('kind') == 'Deployment'
                   and o.get('metadata', {}).get('namespace') == k['namespace']
                   and o.get('metadata', {}).get('name') == k['deployment']]
    if len(deployments) != 1:
        raise ValueError('Controller deployment is ambiguous')
    containers = deployments[0]['spec']['template']['spec']['containers']
    matches = [c for c in containers if c.get('name') == 'manager']
    if len(matches) != 1 or matches[0].get('image') != k['upstream_image']:
        raise ValueError('Controller image is unexpected')
    matches[0]['image'] = k['image']
    prepared = yaml.safe_dump_all(objects, sort_keys=False).encode('utf-8')
    if len(prepared) > MAX_BYTES:
        raise ValueError('Prepared artifact exceeded the bound')
    return prepared, {'schema_version': 1, 'operation': 'prepare-only',
                      'upstream_sha256': hashlib.sha256(raw).hexdigest(),
                      'prepared_sha256': hashlib.sha256(prepared).hexdigest(),
                      'resource_count': len(objects), 'controller_image': k['image'],
                      'cluster_contacted': False, 'installed': False}


def download(profile):
    request = Request(profile['kueue']['manifest_url'], headers={'User-Agent': 'AllotForge-acceptance-preparation'})
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise ValueError('Download failed')
        return response.read(MAX_BYTES + 1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--download', action='store_true', help='Download and verify the one pinned official artifact; does not install it')
    parser.add_argument('--output', help='New local manifest file; existing files are never overwritten')
    args = parser.parse_args(argv)
    try:
        profile = load_profile()
        if not args.download:
            if args.output:
                raise ValueError('Download opt-in is required')
            print(json.dumps({'operation': 'plan-only', 'manifest_url': profile['kueue']['manifest_url'],
                              'sha256': profile['kueue']['manifest_sha256'],
                              'controller_image': profile['kueue']['image'],
                              'cluster_contacted': False, 'downloaded': False, 'installed': False}))
            return 0
        if not args.output or os.path.lexists(args.output):
            raise ValueError('A new output file is required')
        prepared, report = prepare(download(profile), profile)
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(prepared)
        print(json.dumps(report))
        return 0
    except Exception:
        print('Pinned Kueue preparation failed; no cluster operation was attempted. Source and path details withheld.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
