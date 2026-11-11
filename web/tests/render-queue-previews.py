"""Stage 11 queue visibility previews: synthetic fixtures, not browser QA.

Capture with DAVANO_CAPTURE_STATIC=1 node tests/operations.test.cjs first.
Uses the shared static renderer; no browser or cluster interaction occurs.
"""
from pathlib import Path
import importlib.util
import subprocess

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('renderer', Path(__file__).with_name('render-previews.py'))
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)
for name, width, height in [
    ('stage-11-observed-pending-positions', 1440, 1900),
    ('stage-11-unbound-summary-positions', 1440, 1900),
    ('stage-11-unbound-summary-positions', 390, 2500),
    ('stage-11-partial-pending-positions', 1440, 2000),
    ('stage-11-unavailable-pending-positions', 1440, 1500),
    ('stage-11-partial-pending-positions', 390, 2600),
]:
    renderer.render('gap-build/' + name, 11, width=width, height=height,
                    evidence='SYNTHETIC CONTRACT FIXTURE')
    target = ROOT / 'screenshots/gap-build' / (name + ('-mobile' if width < 680 else ''))
    subprocess.run(['pdftoppm', '-png', '-singlefile', '-scale-to', str(max(width, height)),
                    str(target.with_suffix('.pdf')), str(target)], check=True)
