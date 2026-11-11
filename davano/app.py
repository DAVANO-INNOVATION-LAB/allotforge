import os
import hmac
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from .inventory import DemoInventory, KubernetesInventory, InventoryUnavailable
from .models import PoolCreate, PolicyCreate, EvaluationRequest
from .service import FleetService
from .store import Store
from . import __version__

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT if (ROOT / "fixtures" / "cluster.json").exists() else Path(__file__).resolve().parent


def create_app(source=None, db_path=None, inventory=None):
    source = source or os.getenv("DAVANO_SOURCE", "demo")
    if source not in ("demo", "kubernetes"):
        raise ValueError("DAVANO_SOURCE must be demo or kubernetes")
    if inventory is None:
        inventory = DemoInventory(ASSETS / "fixtures" / "cluster.json") if source == "demo" else KubernetesInventory()
    if inventory.source != source:
        raise ValueError("Inventory source mismatch")
    seed = inventory.fixture.get("configuration") if source == "demo" and hasattr(inventory, "fixture") else None
    path = db_path or os.getenv("DAVANO_DB_PATH", str(Path.cwd() / ".data" / f"{source}.db"))
    store = Store(path, seed=seed, source=source)
    fleet = FleetService(inventory, store)

    @asynccontextmanager
    async def lifespan(app):
        yield
        store.close()

    app = FastAPI(title="AllotForge resource API", version=__version__, description="GPU inventory, local advisory planning and explicitly enabled Kubernetes desired-state control. Automatic tenant RBAC grants remain disabled; separately configured project execution uses administrator-approved profiles only.", lifespan=lifespan)
    app.state.fleet = fleet

    execution_enabled = os.getenv("DAVANO_EXECUTION_ENABLED", "false").lower() == "true"
    inventory_admin_token = os.getenv("DAVANO_API_TOKEN", "")

    @app.middleware("http")
    async def browser_safety(request: Request, call_next):
        # Enabling scoped execution must not leave a second unscoped route to
        # live workload data or local configuration. Control has its own auth.
        route = request.url.path
        if execution_enabled and route.startswith("/api/v1/") and not route.startswith(("/api/v1/execution/", "/api/v1/control/")):
            supplied = request.headers.get("authorization", "")
            candidate = supplied[7:] if supplied.startswith("Bearer ") else ""
            if len(inventory_admin_token) < 32 or not hmac.compare_digest(candidate.encode("utf-8"), inventory_admin_token.encode("utf-8")):
                return JSONResponse(status_code=403, content={"detail": "Administrator authentication is required for unscoped inventory and advisory routes while project execution is enabled"})
        if request.method not in ("GET", "HEAD", "OPTIONS"):
            origin = request.headers.get("origin")
            if request.headers.get("sec-fetch-site") == "cross-site" or (origin and urlparse(origin).netloc != request.headers.get("host")):
                return JSONResponse(status_code=403, content={"detail": "Cross-origin mutations are forbidden"})
            if request.headers.get("content-length", "0").isdigit() and int(request.headers.get("content-length", "0")) > 65536:
                return JSONResponse(status_code=413, content={"detail": "Request body too large"})
            if request.method in ("POST", "PUT", "PATCH") and request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
                return JSONResponse(status_code=415, content={"detail": "Use application/json"})
            chunks, size = [], 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > 65536:
                    return JSONResponse(status_code=413, content={"detail": "Request body too large"})
                chunks.append(chunk)
            request._body = b"".join(chunks)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(InventoryUnavailable)
    async def inventory_error(request, error):
        return JSONResponse(status_code=503, content={"detail": str(error), "source": source, "fallback": False})

    @app.get("/healthz")
    def health():
        return {"status": "ok", "version": __version__}

    @app.get("/readyz")
    def ready():
        inventory.snapshot()
        return {"status": "ready", "source": source}

    @app.get("/api/v1/state")
    def state():
        return fleet.state()

    for collection in ("nodes", "pools", "policies", "workloads", "integrations", "observability"):
        def make_endpoint(kind):
            def endpoint():
                return fleet.state()[kind]
            return endpoint
        app.add_api_route(f"/api/v1/{collection}", make_endpoint(collection), methods=["GET"], name=f"list_{collection}")

    @app.get("/api/v1/overview")
    def overview():
        state = fleet.state()
        return {"meta": state["meta"], "summary": state["summary"]}

    def create(kind, body):
        try:
            return {**store.create(kind, body.model_dump()), "enforcement": "advisory-only"}
        except KeyError as e:
            raise HTTPException(404, e.args[0]) from None
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Configuration ID already exists") from None

    @app.post("/api/v1/pools", status_code=201)
    def create_pool(body: PoolCreate):
        return create("pools", body)

    @app.post("/api/v1/policies", status_code=201)
    def create_policy(body: PolicyCreate):
        return create("policies", body)

    @app.delete("/api/v1/{collection}/{item_id}")
    def delete_config(collection: str, item_id: str):
        if collection not in ("pools", "policies"):
            raise HTTPException(404, "Unknown configuration collection")
        try:
            store.delete(collection, item_id)
            return {"deleted": item_id, "cluster_changed": False}
        except KeyError as e:
            raise HTTPException(404, e.args[0]) from None
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Remove associated local policies before deleting this pool") from None

    @app.post("/api/v1/evaluate")
    def evaluate(body: EvaluationRequest):
        try:
            return fleet.evaluate(body)
        except KeyError as e:
            raise HTTPException(404, e.args[0]) from None

    from .control_api import install_control_api
    install_control_api(app, source)

    from .workload_api import install_workload_api
    install_workload_api(app, source, store)

    from .ray_integration import create_ray_router
    app.include_router(create_ray_router(fleet))

    if (ASSETS / "web" / "index.html").exists():
        app.mount("/", StaticFiles(directory=ASSETS / "web", html=True), name="web")
    return app


def main():
    import uvicorn
    uvicorn.run("davano.app:create_app", factory=True, host=os.getenv("DAVANO_BIND", "127.0.0.1"), port=int(os.getenv("PORT", "8080")))


if __name__ == "__main__":
    main()
