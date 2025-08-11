# tasks.py
import os
import time
import datetime
import json
import traceback
import importlib.util
import runpy
from celery_app import celery

# Config: where to look for api call files and mapping file.
API_CALLS_DIR = os.environ.get("API_CALLS_DIR", "/app/api_calls")
MAPPING_FILENAME = os.environ.get("API_CALLS_MAPPING", "mapping.json")
MAPPING_PATH = os.path.join(API_CALLS_DIR, MAPPING_FILENAME)


def load_mapping():
    """
    Load the mapping JSON once per call. Return dict mapping logic_name -> filename.
    If the mapping file doesn't exist or is invalid, return empty dict.
    """
    try:
        with open(MAPPING_PATH, "r", encoding="utf-8") as fh:
            mapping = json.load(fh)
            if not isinstance(mapping, dict):
                raise ValueError("mapping file must contain a JSON object")
            return mapping
    except FileNotFoundError:
        return {}
    except Exception:
        # If mapping is malformed, don't crash the worker; log and return empty mapping.
        traceback.print_exc()
        return {}


def safe_join_api_calls(filename: str) -> str:
    """
    Build an absolute path for the given filename under API_CALLS_DIR.
    Prevent directory traversal by using basename.
    """
    # only allow basename (no subdirectories) to avoid traversal
    safe_name = os.path.basename(filename)
    return os.path.join(API_CALLS_DIR, safe_name)


def run_logic(logic_name: str, params: dict, payload: dict):
    """
    Execute the Python file associated with logic_name.
    Conventions:
    - mapping.json maps logic_name -> python filename (e.g. "validate_input": "validate_input.py")
    - The Python file must expose a function `run(params: dict, payload: dict) -> dict`
      OR a top-level callable named `handler` or `main`. `run` is preferred.
    - Returns a dict: {"ok": True, "result": ...} on success or {"ok": False, "error": "..."} on failure.
    """
    mapping = load_mapping()
    filename = mapping.get(logic_name)
    if not filename:
        return {"ok": False, "error": f"no file mapping for logic '{logic_name}'", "logic": logic_name}

    file_path = safe_join_api_calls(filename)

    if not os.path.exists(file_path):
        return {"ok": False, "error": f"mapped file '{filename}' not found", "path": file_path}

    try:
        # Option A: preferred — load module via importlib so we can call run(...)
        spec = importlib.util.spec_from_file_location(f"api_calls.{logic_name}", file_path)
        module = importlib.util.module_from_spec(spec)
        loader = spec.loader
        if loader is None:
            raise ImportError("no loader for spec")
        loader.exec_module(module)

        # find callable
        if hasattr(module, "run") and callable(module.run):
            result = module.run(params, payload)
        elif hasattr(module, "handler") and callable(module.handler):
            result = module.handler(params, payload)
        elif hasattr(module, "main") and callable(module.main):
            result = module.main(params, payload)
        else:
            return {"ok": False, "error": f"no callable 'run'/'handler'/'main' in {filename}"}

        return {"ok": True, "result": result}
    except Exception as e:
        tb = traceback.format_exc()
        # return both the message and the traceback for debugging
        return {"ok": False, "error": str(e), "traceback": tb}


@celery.task(bind=True, name="execute_pipeline")
def execute_pipeline(self, nodes: list, payload: dict = None, trigger_meta: dict = None):
    """
    Celery task that executes a list of nodes sequentially.
    nodes: list of dicts {id, logic, params}
    payload: incoming payload (webhook body or {}) that may be used by nodes
    trigger_meta: metadata about the trigger
    """
    payload = payload or {}
    trigger_meta = trigger_meta or {}

    results = []
    try:
        for node in nodes:
            logic_name = node.get("logic")
            params = node.get("params", {}) or {}
            # call run_logic which will import and call the file in api_calls
            res = run_logic(logic_name, params, payload)
            results.append({"node_id": node.get("id"), "logic": logic_name, "result": res})

        # If this trigger is a recurring time-trigger, reschedule the next run
        if trigger_meta.get("trigger_type") == "time" and trigger_meta.get("mode") == "interval":
            interval = int(trigger_meta.get("interval_seconds", 60))
            execute_pipeline.apply_async(
                args=[nodes, payload, trigger_meta],
                countdown=interval
            )

        return {"status": "success", "results": results}
    except Exception as exc:
        self.update_state(state="FAILURE", meta={"exc": str(exc)})
        raise

