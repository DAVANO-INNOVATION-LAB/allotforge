"""Stage 12 diagnostics previews: synthetic fixtures, not browser QA.

First run DAVANO_CAPTURE_STATIC=1 node tests/diagnostics.test.cjs.
Uses the shared static renderer without browser or cluster interaction.
"""
from pathlib import Path
import importlib.util
import subprocess

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('renderer', Path(__file__).with_name('render-previews.py'))
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)
for name, width, height in [
    ('stage-12-diagnostics-empty', 1440, 1150),
    ('stage-12-diagnostics-loading', 1440, 1150),
    ('stage-12-diagnostics-observed', 1440, 2450),
    ('stage-12-diagnostics-observed', 390, 4300),
    ('stage-12-diagnostics-stale', 1440, 2450),
    ('stage-12-diagnostics-unauthorized', 1440, 1200),
    ('stage-12-diagnostics-unauthorized', 390, 1800),
    ('stage-12-diagnostics-demo', 1440, 2150),
]:
    renderer.render('gap-build/' + name, 12, width=width, height=height,
                    evidence='SYNTHETIC CONTRACT FIXTURE')
    target = ROOT / 'screenshots/gap-build' / (name + ('-mobile' if width < 680 else ''))
    subprocess.run(['pdftoppm', '-png', '-singlefile', '-scale-to', str(max(width, height)),
                    str(target.with_suffix('.pdf')), str(target)], check=True)
