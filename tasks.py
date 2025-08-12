# tasks.py
import os
import time
import datetime
import json
import traceback
import importlib.util
import tempfile
import subprocess
import sys
import inspect
from typing import Any
from celery_app import celery
import requests
from logger import log_node_start, log_node_output

# Config: where to look for api call files and mapping file.
API_CALLS_DIR = os.environ.get("API_CALLS_DIR", "/app/api_calls")
MAPPING_FILENAME = os.environ.get("API_CALLS_MAPPING", "mapping.json")
MAPPING_PATH = os.path.join(API_CALLS_DIR, MAPPING_FILENAME)

# Executor service URL (executor container)
EXECUTOR_URL = os.environ.get("EXECUTOR_URL", "http://executor:8001/run")

# Execution limits for local fallback (not used for custom nodes in worker)
CUSTOM_TIMEOUT = int(os.environ.get("CUSTOM_TIMEOUT_SECONDS", "5"))

REQUEST_TIMEOUT = int(os.environ.get("EXECUTOR_REQUEST_TIMEOUT", "10"))  # seconds for HTTP calls to executor

def load_mapping():
    try:
        with open(MAPPING_PATH, "r", encoding="utf-8") as fh:
            mapping = json.load(fh)
            if not isinstance(mapping, dict):
                raise ValueError("mapping file must contain a JSON object")
            return mapping
    except FileNotFoundError:
        return {}
    except Exception:
        traceback.print_exc()
        return {}


def safe_join_api_calls(filename: str) -> str:
    safe_name = os.path.basename(filename)
    return os.path.join(API_CALLS_DIR, safe_name)


def _call_module_callable(module, prev: Any, params: dict, payload: dict):
    """
    Call module.run/handler/main with flexible signature:
    - prefer fn(prev, params, payload)
    - fallback to fn(params, payload)
    - fallback to fn(payload) / fn(params) / fn()
    """
    fn = None
    if hasattr(module, "run") and callable(module.run):
        fn = module.run
    elif hasattr(module, "handler") and callable(module.handler):
        fn = module.handler
    elif hasattr(module, "main") and callable(module.main):
        fn = module.main
    else:
        raise AttributeError("no callable 'run'/'handler'/'main' in module")

    sig = inspect.signature(fn)
    pcount = len(sig.parameters)

    if pcount >= 3:
        return fn(prev, params, payload)
    elif pcount == 2:
        return fn(params, payload)
    elif pcount == 1:
        # prefer payload if it looks like the last param
        return fn(payload)
    else:
        return fn()


def _execute_api_call_file(file_path: str, prev: Any, params: dict, payload: dict):
    """
    Import a python file as module and call its entrypoint.
    """
    try:
        spec = importlib.util.spec_from_file_location(
            f"api_calls.exec_{int(time.time()*1000)}", file_path
        )
        module = importlib.util.module_from_spec(spec)
        loader = spec.loader
        if loader is None:
            raise ImportError("no loader for spec")
        loader.exec_module(module)

        result = _call_module_callable(module, prev, params, payload)
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}


def _call_executor_service(code_str: str, prev: Any, params: dict, payload: dict):
    """
    Send code + context to the executor container via HTTP. Executor runs code and returns JSON.
    """
    try:
        body = {
            "code": code_str,
            "prev": prev,
            "params": params,
            "payload": payload,
            "timeout_seconds": CUSTOM_TIMEOUT
        }
        resp = requests.post(EXECUTOR_URL, json=body, timeout=REQUEST_TIMEOUT)
        # Expect JSON response
        try:
            data = resp.json()
        except Exception:
            return {"ok": False, "error": "executor returned non-json", "status_code": resp.status_code, "text": resp.text}

        return data
    except requests.exceptions.RequestException as re:
        return {"ok": False, "error": f"executor request failed: {str(re)}"}


def run_logic(logic_name: str, params: dict, payload: dict, prev_output: Any, node_meta: dict = None):
    """
    Execute logic for a node.

    - File-based: mapping.json -> filename -> import and call.
      Module entrypoint can accept (prev, params, payload), (params,payload) etc.
      `prev_output` is passed as the first positional argument when possible.

    - Custom logic: send code string to executor service and return its JSON response.
      Custom node is detected when logic_name == "custom" or params['_type'] == 'custom'
    """
    # detect custom node
    is_custom = False
    if logic_name == "custom":
        is_custom = True
    if isinstance(params, dict) and params.get("_type") == "custom":
        is_custom = True

    # check for explicit node tag that disables pass-through
    no_pass = False
    if node_meta:
        # check several conventions: boolean "NO PASS THROUGH", "no_pass_through", tags list
        if node_meta.get("NO PASS THROUGH") is True or node_meta.get("no_pass_through") is True:
            no_pass = True
        tags = node_meta.get("tags")
        if isinstance(tags, list) and "NO PASS THROUGH" in tags:
            no_pass = True

    # If pass-through disabled, pass None as prev
    prev_to_pass = None if no_pass else prev_output

    if is_custom:
        # expect params['code'] with source
        code_str = None
        if isinstance(params, dict):
            code_str = params.get("code")
            if code_str is None and isinstance(params.get("args"), list) and len(params["args"]) > 0:
                code_str = params["args"][0]
        if not code_str:
            return {"ok": False, "error": "custom node requires params['code'] (string)"}

        return _call_executor_service(code_str, prev_to_pass, params, payload)

    # file-based mapping
    mapping = load_mapping()
    filename = mapping.get(logic_name)
    if not filename:
        return {"ok": False, "error": f"no file mapping for logic '{logic_name}'", "logic": logic_name}

    file_path = safe_join_api_calls(filename)
    if not os.path.exists(file_path):
        return {"ok": False, "error": f"mapped file '{filename}' not found", "path": file_path}

    return _execute_api_call_file(file_path, prev_to_pass, params, payload)


@celery.task(bind=True, name="execute_pipeline")
def execute_pipeline(self, nodes: list, payload: dict = None, trigger_meta: dict = None):
    """
    Executes nodes sequentially. Passes previous node output as first positional argument into next node
    unless NO PASS THROUGH is set on the next node.
    """
    payload = payload
    trigger_meta = trigger_meta or {}

    prev_output = payload  # initial previous output is the incoming payload
    results = []
    try:
        for node in nodes:
            node_id = node.get("id")
            logic_name = node.get("logic")
            params = node.get("params", {}) or {}
            # node_meta can include tags or direct flags
            node_meta = node
            log_node_start(node_id, logic_name, params)
            # Support backward-compatible inline type marker
            if node.get("type") == "custom":
                logic_name = "custom"

            res = run_logic(logic_name, params, payload, prev_output, node_meta=node_meta)
            log_node_output(node_id, res)
            results.append({"node_id": node_id, "logic": logic_name, "result": res})

            # Determine what becomes prev_output for next node:
            # If run returns {"ok": True, "result": something} -> use something
            # If run returns {"ok": True, "stdout": ...} (executor style) -> use stdout
            # Else: use the entire res object
            next_prev = None
            if isinstance(res, dict):
                if res.get("ok") is True and "result" in res:
                    next_prev = res.get("result")
                elif res.get("ok") is True and "stdout" in res:
                    next_prev = res.get("stdout")
                else:
                    # fallback to the raw result dict
                    next_prev = res
            else:
                next_prev = res

            # Update prev_output for next iteration unless the next node disables pass-through;
            # Decision about disabling is done at the start of next loop (in run_logic).
            prev_output = next_prev

        # recurring scheduling unchanged
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

