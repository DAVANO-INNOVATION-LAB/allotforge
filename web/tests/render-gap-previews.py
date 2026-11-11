"""Labelled static renders of exercised UI DOM, never represented as browser QA."""
from pathlib import Path
import importlib.util
import subprocess
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('renderer', Path(__file__).with_name('render-previews.py'))
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)
for name, stage, width, height in [
    ('stage-1-safe-workload-launch', 1, 1440, 1250),
    ('stage-2-telemetry-disabled', 2, 1440, 1600),
    ('stage-3-reviewed-job-preview', 3, 1440, 2200),
    ('stage-4-signal-evidence-fixture', 4, 1440, 2200),
    ('stage-5-queue-reasons-fixture', 5, 1440, 1250),
    ('stage-1-safe-workload-launch', 1, 390, 1750),
    ('stage-2-telemetry-disabled', 2, 390, 2000),
]:
    renderer.render('gap-build/'+name, stage, width=width, height=height,
        evidence='SYNTHETIC UI CONTRACT FIXTURE · NO LIVE CLUSTER')
    target = ROOT / 'screenshots/gap-build' / (name + ('-mobile' if width < 680 else ''))
    subprocess.run(['pdftoppm','-png','-singlefile','-scale-to',str(max(width,height)),str(target.with_suffix('.pdf')),str(target)],check=True)

for name, stage, height, modal in [
    ('stage-6-resource-assignment', 6, 1700, False),
    ('stage-7-namespace-quota-preview', 7, 1550, True),
    ('stage-8-project-workload-move-preview', 8, 1900, True),
    ('stage-9-reassignment-awaiting-drain', 9, 2200, False),
]:
    renderer.render('gap-build/'+name, stage, width=1440, height=height, modal=modal,
        evidence='SYNTHETIC UI CONTRACT FIXTURE · NO LIVE CLUSTER')
    target = ROOT / 'screenshots/gap-build' / name
    subprocess.run(['pdftoppm','-png','-singlefile','-scale-to',str(height),str(target.with_suffix('.pdf')),str(target)],check=True)
