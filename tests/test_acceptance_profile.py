"""Pinned fixture preparation only. These tests never install or contact a cluster."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scripts import prepare_kueue_acceptance as prepare


class AcceptanceProfileTests(unittest.TestCase):
    def fixture(self):
        profile = prepare.load_profile()
        objects = [{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kueue-system'}},
                   {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                    'metadata': {'namespace': 'kueue-system', 'name': 'kueue-controller-manager'},
                    'spec': {'template': {'spec': {'containers': [{'name': 'manager', 'image': profile['kueue']['upstream_image']}]}}}}]
        raw = yaml.safe_dump_all(objects).encode()
        profile['kueue']['manifest_bytes'] = len(raw)
        profile['kueue']['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
        return raw, profile, objects

    def test_catalog_is_explicit_supported_target(self):
        profile = prepare.load_profile()
        self.assertEqual(profile['kind']['kubernetes_version'], 'v1.34.11')
        self.assertEqual(profile['kind']['context'], 'kind-allotforge-acceptance')
        self.assertEqual(profile['kueue']['api_version'], 'v1beta2')
        self.assertEqual(profile['kueue']['manifest_bytes'], 1798254)
        self.assertEqual(profile['limits']['gpu_workloads'], 0)
        self.assertFalse(profile['limits']['paid_runners'])
        self.assertFalse(profile['limits']['physical_gpu_validation'])

    def test_runner_and_catalog_pins_cannot_silently_diverge(self):
        from acceptance import cluster
        profile = prepare.load_profile()
        self.assertEqual(cluster.CONTEXT, profile['kind']['context'])
        self.assertEqual(cluster.KUBERNETES, profile['kind']['kubernetes_version'])
        self.assertEqual('v' + cluster.KUEUE_VERSION, profile['kueue']['version'])
        self.assertEqual(cluster.API, 'kueue.x-k8s.io/' + profile['kueue']['api_version'])
        self.assertEqual(cluster.KUEUE_IMAGE, profile['kueue']['image'])

    def test_pins_only_exact_manager_image_and_retains_semantics(self):
        raw, profile, objects = self.fixture()
        result, report = prepare.prepare(raw, profile)
        expected = copy.deepcopy(objects)
        expected[1]['spec']['template']['spec']['containers'][0]['image'] = profile['kueue']['image']
        self.assertEqual(list(yaml.safe_load_all(result)), expected)
        self.assertEqual(report['prepared_sha256'], hashlib.sha256(result).hexdigest())
        self.assertEqual(report['upstream_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(report['resource_count'], 2)
        self.assertFalse(report['installed'])
        self.assertFalse(report['cluster_contacted'])

    def test_rejects_hash_size_and_oversized_data_before_yaml_parsing(self):
        raw, profile, _ = self.fixture()
        for bad in (raw+b' ', b'not yaml', b'x'*(prepare.MAX_BYTES+1)):
            with self.subTest(size=len(bad)), patch.object(prepare.yaml, 'safe_load_all') as parse:
                with self.assertRaises(ValueError):
                    prepare.prepare(bad, profile)
                parse.assert_not_called()

    def test_rejects_missing_duplicate_or_unexpected_controller(self):
        _, profile, objects = self.fixture()
        cases = [[], objects[:1], objects + [copy.deepcopy(objects[1])], [None], ['not an object']]
        wrong = copy.deepcopy(objects)
        wrong[1]['spec']['template']['spec']['containers'][0]['image'] = 'wrong.invalid/image'
        cases.append(wrong)
        wrong = copy.deepcopy(objects)
        wrong[1]['spec']['template']['spec']['containers'].append(copy.deepcopy(wrong[1]['spec']['template']['spec']['containers'][0]))
        cases.append(wrong)
        for candidate in cases:
            raw = yaml.safe_dump_all(candidate).encode()
            profile['kueue'].update(manifest_bytes=len(raw), manifest_sha256=hashlib.sha256(raw).hexdigest())
            with self.subTest(candidate=candidate), self.assertRaises((ValueError, TypeError)):
                prepare.prepare(raw, profile)

    def test_offline_default_makes_no_download(self):
        with patch.object(prepare, 'download') as fetch, patch('sys.stdout', new_callable=io.StringIO) as stdout:
            self.assertEqual(prepare.main([]), 0)
            self.assertEqual(json.loads(stdout.getvalue())['operation'], 'plan-only')
            fetch.assert_not_called()

    def test_output_requires_download_opt_in(self):
        with patch.object(prepare, 'download') as fetch, patch('sys.stderr', new_callable=io.StringIO):
            self.assertEqual(prepare.main(['--output', 'unused']), 2)
            fetch.assert_not_called()

    def test_existing_file_or_symlink_refused_before_download(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'manifest.yaml'
            path.write_text('existing')
            link = Path(root)/'link.yaml'
            link.symlink_to(path)
            broken = Path(root)/'broken.yaml'
            broken.symlink_to(Path(root)/'absent')
            for target in (path, link, broken):
                with patch.object(prepare, 'download') as fetch, patch('sys.stderr', new_callable=io.StringIO):
                    self.assertEqual(prepare.main(['--download', '--output', str(target)]), 2)
                    fetch.assert_not_called()
            self.assertEqual(path.read_text(), 'existing')

    def test_new_output_private_and_report_contains_no_paths(self):
        raw, profile, _ = self.fixture()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'private-manifest.yaml'
            with patch.object(prepare, 'load_profile', return_value=profile), patch.object(prepare, 'download', return_value=raw), patch('sys.stdout', new_callable=io.StringIO) as stdout:
                self.assertEqual(prepare.main(['--download', '--output', str(path)]), 0)
                self.assertNotIn(root, stdout.getvalue())
                self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
                self.assertFalse(json.loads(stdout.getvalue())['installed'])

    def test_download_failure_sanitizes_details(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'new.yaml'
            with patch.object(prepare, 'download', side_effect=RuntimeError('private-credential-detail')), patch('sys.stderr', new_callable=io.StringIO) as stderr:
                self.assertEqual(prepare.main(['--download', '--output', str(path)]), 2)
                self.assertNotIn('private-credential-detail', stderr.getvalue())
                self.assertNotIn(root, stderr.getvalue())
                self.assertFalse(path.exists())


class AcceptanceWorkflowTests(unittest.TestCase):
    root = Path(__file__).resolve().parents[1]

    def test_exact_pins_read_only_identity_and_standard_runner(self):
        profile = prepare.load_profile()
        workflow = yaml.load((self.root/'.github/workflows/kueue-acceptance.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(workflow['permissions'], {'contents': 'read'})
        self.assertEqual(set(workflow['on']), {'workflow_call', 'workflow_dispatch', 'pull_request'})
        job = workflow['jobs']['kueue-acceptance']
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        self.assertEqual(int(job['timeout-minutes']), profile['limits']['ci_job_minutes'])
        self.assertEqual(job['env']['DAVANO_KUEUE_API_VERSION'], profile['kueue']['api_version'])
        steps = job['steps']
        for step in steps:
            if 'uses' in step:
                self.assertRegex(step['uses'], r'^[A-Za-z0-9/-]+@[a-f0-9]{40}$')
        checkout = next(s for s in steps if s.get('uses', '').startswith('actions/checkout@'))
        self.assertEqual(checkout['with'], {'ref': '${{ github.sha }}', 'persist-credentials': 'false'})
        kind = next(s for s in steps if s.get('id') == 'kind')['with']
        self.assertEqual(kind['version'], profile['kind']['version'])
        self.assertEqual(kind['node_image'], profile['kind']['node_image'])
        self.assertEqual(kind['kubectl_version'], profile['kind']['kubernetes_version'])
        self.assertEqual(kind['cluster_name'], profile['kind']['cluster_name'])
        self.assertEqual(kind['kubeconfig'], '${{ runner.temp }}/allotforge-kind.yaml')

    def test_private_binding_explicit_commands_and_narrow_artifact(self):
        workflow = yaml.load((self.root/'.github/workflows/kueue-acceptance.yml').read_text(), Loader=yaml.BaseLoader)
        steps = workflow['jobs']['kueue-acceptance']['steps']
        binding = next(s for s in steps if s.get('name', '').startswith('Bind private'))
        self.assertIn('umask 077', binding['run'])
        self.assertIn('config view --raw -o json > "$PRIVATE_CONFIG"', binding['run'])
        self.assertIn('target.hostname == \'127.0.0.1\'', binding['run'])
        execute = next(s for s in steps if s.get('name', '').startswith('Exercise generated'))
        for flag in ('--execute', '--kubeconfig-json', '--expected-server', '--expected-kube-system-uid', '--expected-kubernetes-version', '--evidence'):
            self.assertIn(flag, execute['run'])
        for step in steps:
            for line in step.get('run', '').splitlines():
                if line.strip().startswith('kubectl '):
                    self.assertIn('--kubeconfig ', line)
                    if 'config view' not in line:
                        self.assertIn('--context kind-allotforge-acceptance', line)
        artifact = next(s for s in steps if s.get('uses', '').startswith('actions/upload-artifact@'))
        self.assertEqual(artifact['with']['path'], '${{ runner.temp }}/allotforge-acceptance-evidence.json')
        self.assertEqual(artifact['with']['retention-days'], '14')
        teardown = next(s for s in steps if s.get('name', '').startswith('Destroy the dedicated'))
        self.assertEqual(teardown['if'], "always() && steps.kind.outcome == 'success'")
        self.assertIn('timeout 90s kind delete cluster --name allotforge-acceptance', teardown['run'])
        self.assertIn('clusters=$(kind get clusters)', teardown['run'])

    def test_release_build_requires_acceptance_and_no_extra_write_permission(self):
        workflow = yaml.load((self.root/'.github/workflows/release.yml').read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(workflow['permissions'], {'contents': 'read'})
        self.assertEqual(workflow['jobs']['kueue-acceptance'], {
            'uses': './.github/workflows/kueue-acceptance.yml', 'permissions': {'contents': 'read'}})
        self.assertIn('kueue-acceptance', workflow['jobs']['build']['needs'])
        for name, job in workflow['jobs'].items():
            if name != 'publish':
                self.assertNotEqual(job.get('permissions', {}).get('contents'), 'write')


if __name__ == '__main__':
    unittest.main()
