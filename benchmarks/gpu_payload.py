"""Run only inside an approved, digest-pinned PyTorch GPU image.

No downloads, model weights, cluster credentials or CPU fallback. This is a
single-GPU CUDA matmul microbenchmark, not an inference or training benchmark.
"""
import json
import os
import statistics
import time
from datetime import datetime, timezone


def main():
    import torch
    if not torch.cuda.is_available() or torch.cuda.device_count() != 1:
        raise RuntimeError("Exactly one visible CUDA GPU is required; no CPU fallback")
    size, iterations, warmup = 4096, 100, 10
    torch.manual_seed(20261111)
    torch.cuda.manual_seed_all(20261111)
    torch.backends.cuda.matmul.allow_tf32 = False
    a = torch.randn((size, size), device="cuda", dtype=torch.float16)
    b = torch.randn((size, size), device="cuda", dtype=torch.float16)
    out = torch.empty_like(a)
    for _ in range(warmup):
        torch.mm(a, b, out=out)
    torch.cuda.synchronize()
    samples = []
    started = datetime.now(timezone.utc).isoformat()
    for _ in range(iterations):
        t = time.perf_counter_ns()
        torch.mm(a, b, out=out)
        torch.cuda.synchronize()
        samples.append((time.perf_counter_ns() - t) / 1e6)
    # Correctness smoke check, excluded from timings. Exact hardware parity and
    # full numerical correctness are separate gates; this only detects NaNs.
    finite = bool(torch.isfinite(out).all().item())
    prop = torch.cuda.get_device_properties(0)
    print(json.dumps({
        "schema_version": 1, "evidence_type": "live-gpu-payload",
        "suite": "cuda-fp16-matmul-4096/v1", "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "sample_id": os.environ.get("DAVANO_BENCH_SAMPLE", "unassigned"),
        "gpu_name": prop.name, "gpu_memory_bytes": prop.total_memory,
        "torch_version": torch.__version__, "cuda_runtime": torch.version.cuda,
        "visible_gpu_count": torch.cuda.device_count(), "output_finite": finite,
        "warmup_iterations": warmup, "iterations": iterations,
        "step_ms": samples, "mean_step_ms": statistics.mean(samples),
        "scope": "CUDA execution plus per-step host synchronization; not scheduler latency",
    }), flush=True)
    if not finite:
        raise RuntimeError("Non-finite GPU output")


if __name__ == "__main__":
    main()
