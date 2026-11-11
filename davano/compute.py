"""Conservative scheduler-request accounting, not CPU/RAM utilization.

Requests round up and capacity rounds down to displayed units. Unsupported
Pod-level budgets and resize state fail closed for the affected dimension.
"""
import re
from decimal import Decimal, DecimalException, ROUND_CEILING, ROUND_FLOOR, localcontext

DIMENSIONS = {"cpu_millicores": ("cpu", Decimal(1000)), "memory_mib": ("memory", Decimal(1) / Decimal(1048576))}


def compute_quantity(value, dimension, *, request=False):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    raw = str(value)
    if len(raw) > 100:
        return None
    match = re.fullmatch(r"([+]?[0-9]+(?:\.[0-9]+)?)([numkMGTPE]|[KMGTPE]i|[eE][+-]?[0-9]+)?", raw)
    if not match:
        return None
    suffix = match[2] or ""
    scales = {"": Decimal(1), "n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal("1e-3"), "k": Decimal(1000)}
    scales.update({x: Decimal(1000) ** (i + 2) for i, x in enumerate("MGTPE")})
    scales.update({x + "i": Decimal(1024) ** (i + 1) for i, x in enumerate("KMGTPE")})
    try:
        with localcontext() as ctx:
            ctx.prec = 160
            if len(suffix) > 1 and suffix.startswith(("e", "E")):
                exponent = int(suffix[1:])
                if not -30 <= exponent <= 30:
                    return None
                scale = Decimal(10) ** exponent
            else:
                scale = scales[suffix]
            amount = Decimal(match[1]) * scale
            if not 0 <= amount <= 2**63 - 1:
                return None
            units = amount * DIMENSIONS[dimension][1]
            return int(units.to_integral_value(rounding=ROUND_CEILING if request else ROUND_FLOOR))
    except (DecimalException, ValueError, KeyError, OverflowError):
        return None


def container_request(container, dimension):
    if not isinstance(container, dict):
        return None
    resources = container.get("resources", {})
    if not isinstance(resources, dict):
        return None
    requests, limits = resources.get("requests", {}), resources.get("limits", {})
    if not isinstance(requests, dict) or not isinstance(limits, dict):
        return None
    key = DIMENSIONS[dimension][0]
    return compute_quantity(requests.get(key, limits.get(key, "0")), dimension, request=True)


def pod_compute_request(pod, dimension):
    spec, status = pod.get("spec", {}), pod.get("status", {})
    key = DIMENSIONS[dimension][0]
    # Pod-level resource budgets and in-place resize may differ from container
    # requests. Do not invent effective requests for these evolving APIs.
    budget = spec.get("resources", {})
    if not isinstance(budget, dict):
        return None
    for section in ("requests", "limits"):
        values = budget.get(section, {})
        if not isinstance(values, dict) or key in values:
            return None
    if status.get("resize"):
        return None
    conditions = status.get("conditions", [])
    if not isinstance(conditions, list) or any(not isinstance(c, dict) for c in conditions):
        return None
    if any(c.get("type") in ("PodResizePending", "PodResizeInProgress") and c.get("status") == "True"
           for c in conditions):
        return None
    for group in ("containerStatuses", "initContainerStatuses"):
        statuses = status.get(group, [])
        if not isinstance(statuses, list):
            return None
        for entry in statuses:
            if not isinstance(entry, dict):
                return None
            allocated = entry.get("allocatedResources", {})
            resources = entry.get("resources", {})
            if not isinstance(allocated, dict) or not isinstance(resources, dict):
                return None
            if key in allocated:
                return None
            for section in ("requests", "limits"):
                values = resources.get(section, {})
                if not isinstance(values, dict) or key in values:
                    return None
    apps, inits = spec.get("containers", []), spec.get("initContainers", [])
    overhead = spec.get("overhead", {})
    if not isinstance(apps, list) or not isinstance(inits, list) or not isinstance(overhead, dict):
        return None
    app_values = [container_request(c, dimension) for c in apps]
    init_values = [container_request(c, dimension) for c in inits]
    extra = compute_quantity(overhead.get(key, "0"), dimension, request=True)
    if extra is None or any(v is None for v in app_values + init_values):
        return None
    sidecars = peak = 0
    for container, amount in zip(inits, init_values):
        if container.get("restartPolicy") == "Always":
            sidecars += amount
            peak = max(peak, sidecars)
        else:
            peak = max(peak, sidecars + amount)
    return max(sum(app_values) + sidecars, peak) + extra


def compute_usage(pods):
    usage = {}
    for pod in pods:
        if pod.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        name = pod.get("spec", {}).get("nodeName")
        if not name:
            continue
        totals = usage.setdefault(name, dict.fromkeys(DIMENSIONS, 0))
        for dimension in DIMENSIONS:
            amount = pod_compute_request(pod, dimension)
            totals[dimension] = None if amount is None or totals[dimension] is None else totals[dimension] + amount
    return usage


def node_compute(status, usage, warnings):
    out = {}
    for dimension, (key, _) in DIMENSIONS.items():
        capacity = compute_quantity(status.get("capacity", {}).get(key), dimension)
        allocatable = compute_quantity(status.get("allocatable", {}).get(key), dimension)
        requested = usage.get(dimension, 0)
        available = None if allocatable is None or requested is None else max(0, allocatable - requested)
        if available is None:
            warnings.append(f"Unknown {key} allocatable or observed requests; {key} fit cannot be verified.")
        if allocatable is not None and requested is not None and requested > allocatable:
            warnings.append(f"Observed {key} requests exceed allocatable resources.")
        out[dimension] = {"capacity": capacity, "allocatable": allocatable, "requested": requested, "available": available}
    return out
