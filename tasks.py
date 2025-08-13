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
import logging
from urllib.parse import quote
try:
    from requests_unixsocket import Session as UnixSession
except Exception:
    UnixSession = None

# ---- config ---------------------------------------------------------
API_CALLS_DIR = os.environ.get("API_CALLS_DIR", "/app/api_calls")
MAPPING_FILENAME = os.environ.get("API_CALLS_MAPPING", "mappings.json")
MAPPING_PATH = os.path.join(API_CALLS_DIR, MAPPING_FILENAME)

EXECUTOR_URL = os.environ.get("EXECUTOR_URL", "http://executor:8001/run")
CUSTOM_TIMEOUT = int(os.environ.get("CUSTOM_TIMEOUT_SECONDS", "5"))
REQUEST_TIMEOUT = int(os.environ.get("EXECUTOR_REQUEST_TIMEOUT", "10"))

# --------------------------------------------------------------------

# setup logging
logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(levelname)s %(message)s")

if EXECUTOR_URL and EXECUTOR_URL.startswith("unix://"):
    if UnixSession is None:
        raise RuntimeError("requests-unixsocket is required for unix sockets. Add to requirements.txt")
    _uds_path = EXECUTOR_URL[len("unix://"):]  # e.g. /tmp/executor_sock/executor.sock
    _session = UnixSession()
    # set an HTTPAdapter-like pool if wanted (requests_unixsocket uses requests under the hood)
    _executor_base = "http+unix://" + quote(_uds_path, safe="")
    logging.debug("Using UDS executor, encoded base=%s", _executor_base)
else:
    _session = requests.Session()
    _adapter = requests.adapters.HTTPAdapter(pool_maxsize=50)
    _session.mount("http://", _adapter)
    _session.mount("https://", _adapter)
    _executor_base = EXECUTOR_URL.rstrip("/")

def load_mapping():
    """Load mappings.json (or configured filename). Returns dict or {}."""
    try:
        with open(MAPPING_PATH, "r", encoding="utf-8") as fh:
            mapping = json.load(fh)
            if not isinstance(mapping, dict):
                logging.warning("mapping file exists but is not a JSON object")
                return {}
            return mapping
    except FileNotFoundError:
        logging.debug("mapping file not found at %s", MAPPING_PATH)
        return {}
    except Exception:
        logging.error("failed loading mapping: %s", traceback.format_exc())
        return {}


def safe_join_api_calls(filename: str) -> str:
    safe_name = os.path.basename(filename)
    return os.path.join(API_CALLS_DIR, safe_name)


def _call_module_callable(module, prev: Any, params: dict, payload: dict):
    """
    Call module.run/handler/main with flexible signature:
      prefer fn(prev, params, payload)
      fallback to fn(params, payload)
      fallback to fn(payload) / fn(params) / fn()
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

    # Call with the best matching signature
    if pcount >= 3:
        return fn(prev, params, payload)
    elif pcount == 2:
        return fn(params, payload)
    elif pcount == 1:
        return fn(payload)
    else:
        return fn()


def _execute_api_call_file(file_path: str, prev: Any, params: dict, payload: dict):
    """
    Import python file as module and call entrypoint. Returns dict with ok/result or error.
    This function logs what it's doing to make debugging straightforward.
    """
    try:
        if not os.path.exists(file_path):
            return {"ok": False, "error": f"File not found: {file_path}"}

        logging.debug("Loading file: %s", file_path)

        # debug: show file preview (first 200 chars)
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                content = fh.read()
                logging.debug("File content length: %d chars", len(content))
                logging.debug("File preview: %s", content[:200].replace("\n", "\\n"))
        except Exception:
            logging.debug("Could not read file content for preview")

        spec = importlib.util.spec_from_file_location(f"api_calls.exec_{int(time.time() * 1000)}", file_path)
        if spec is None:
            return {"ok": False, "error": f"Could not create spec for {file_path}"}

        module = importlib.util.module_from_spec(spec)
        loader = spec.loader
        if loader is None:
            return {"ok": False, "error": "no loader for spec"}

        logging.debug("Executing module...")
        loader.exec_module(module)
        logging.debug("Module executed successfully")

        module_attrs = [a for a in dir(module) if not a.startswith("_")]
        logging.debug("Module attributes: %s", module_attrs)

        # Call the function and capture its return value
        result = _call_module_callable(module, prev, params, payload)
        logging.debug("Module returned: %r", result)

        return {"ok": True, "result": result}
    except Exception as e:
        logging.error("Error in _execute_api_call_file: %s", traceback.format_exc())
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}


def _call_executor_service(code_str: str, prev: Any, params: dict, payload: dict):
    """
    Timed call to executor. Returns executor JSON or error dict.
    Logs timings to help diagnose slowness.
    """
    body = {
        "code": code_str,
        "prev": prev,
        "params": params,
        "payload": payload,
        "timeout_seconds": CUSTOM_TIMEOUT,
    }

    url = f"{_executor_base}/run"
    t0 = time.perf_counter()
    try:
        resp = _session.post(url, json=body, timeout=REQUEST_TIMEOUT)
        t1 = time.perf_counter()
        # try parse json
        try:
            data = resp.json()
        except Exception:
            data = {"ok": False, "error": "executor returned non-json", "status_code": resp.status_code, "text": resp.text}
        t2 = time.perf_counter()
        logging.debug("Executor timings: total=%.2fms request=%.2fms parse=%.2fms url=%s",
                      (t2 - t0) * 1000.0, (t1 - t0) * 1000.0, (t2 - t1) * 1000.0, url)
        return data
    except Exception as ex:
        t_err = time.perf_counter()
        logging.error("Executor request failed after %.2fms: %s", (t_err - t0) * 1000.0, str(ex))
        return {"ok": False, "error": f"executor request failed: {str(ex)}"}

def _coerce_prev(prev):
    """
    Try to coerce prev into a Python object if it's a JSON string / bytes.
    Otherwise return as-is.
    """
    if isinstance(prev, (bytes, bytearray)):
        try:
            txt = prev.decode("utf-8")
            return json.loads(txt)
        except Exception:
            try:
                return prev.decode("utf-8")
            except Exception:
                return prev
    if isinstance(prev, str):
        # try parse JSON
        try:
            return json.loads(prev)
        except Exception:
            return prev
    return prev


def run_logic(logic_name: str, params: dict, payload: dict, prev_output: Any, node_meta: dict = None):
    """
    Execute a node's logic.
    - file-based: lookup mapping -> filename, fallback to logic_name + '.py' if missing
    - custom: send code to executor
    """
    # detect custom node
    is_custom = False
    if logic_name == "custom":
        is_custom = True
    if isinstance(params, dict) and params.get("_type") == "custom":
        is_custom = True

    # detect NO PASS THROUGH
    no_pass = False
    if node_meta:
        if node_meta.get("NO PASS THROUGH") is True or node_meta.get("no_pass_through") is True:
            no_pass = True
        tags = node_meta.get("tags")
        if isinstance(tags, list) and "NO PASS THROUGH" in tags:
            no_pass = True

    prev_to_pass = None if no_pass else prev_output
    # coerce if string/bytes
    prev_to_pass = _coerce_prev(prev_to_pass)

    if is_custom:
        # expect params['code']
        code_str = None
        if isinstance(params, dict):
            code_str = params.get("code")
            if code_str is None and isinstance(params.get("args"), list) and len(params["args"]) > 0:
                code_str = params["args"][0]
        if not code_str:
            return {"ok": False, "error": "custom node requires params['code'] (string)"}
        return _call_executor_service(code_str, prev_to_pass, params, payload)

    # file-based
    mapping = load_mapping()
    filename = mapping.get(logic_name)
    if not filename:
        # fallback guess: logic_name.py in api_calls dir
        guessed = os.path.join(API_CALLS_DIR, f"{logic_name}.py")
        if os.path.exists(guessed):
            filename = f"{logic_name}.py"
            logging.debug("Mapping missing for '%s' - falling back to %s", logic_name, guessed)
        else:
            logging.debug("No mapping and no fallback file for logic '%s'", logic_name)
            return {"ok": False, "error": f"no file mapping for logic '{logic_name}'", "logic": logic_name}

    file_path = safe_join_api_calls(filename)
    if not os.path.exists(file_path):
        logging.error("Mapped file not found at path: %s", file_path)
        return {"ok": False, "error": f"mapped file '{filename}' not found", "path": file_path}

    # debug: log the prev being passed to the module
    logging.debug("About to execute file %s with prev type=%s prev=%r params=%r payload_keys=%s",
                  file_path, type(prev_to_pass).__name__, repr(prev_to_pass),
                  params, list(payload.keys()) if isinstance(payload, dict) else None)

    return _execute_api_call_file(file_path, prev_to_pass, params, payload)


@celery.task(bind=True, name="execute_pipeline")
def execute_pipeline(self, nodes: list, payload: dict = None, trigger_meta: dict = None):
    """
    Executes nodes sequentially. The previous node's output is passed into the next one (unless NO PASS THROUGH).
    """
    # ensure payload is a dict (avoid None)
    payload = payload if payload is not None else {}
    trigger_meta = trigger_meta or {}

    prev_output = payload
    results = []
    try:
        logging.info("Pipeline started trigger_meta=%s", trigger_meta)
        for idx, node in enumerate(nodes):
            node_id = node.get("id")
            logic_name = node.get("logic")
            params = node.get("params", {}) or {}
            node_meta = node
            if node.get("type") == "custom":
                logic_name = "custom"

            logging.debug("Running node #%d id=%s logic=%s", idx, node_id, logic_name)
            # coerce prev for safety
            coerced_prev = _coerce_prev(prev_output)
            res = run_logic(logic_name, params, payload, coerced_prev, node_meta=node_meta)
            logging.debug("Node %s returned: %r", node_id, res)

            results.append({"node_id": node_id, "logic": logic_name, "result": res})

            # derive next_prev
            next_prev = None
            if isinstance(res, dict):
                if res.get("ok") is True and "result" in res:
                    next_prev = res.get("result")
                elif res.get("ok") is True and "stdout" in res:
                    next_prev = res.get("stdout")
                else:
                    next_prev = res
            else:
                next_prev = res

            prev_output = next_prev

        # handle recurring time trigger reschedule
        if trigger_meta.get("trigger_type") == "time" and trigger_meta.get("mode") == "interval":
            interval = int(trigger_meta.get("interval_seconds", 60))
            execute_pipeline.apply_async(
                args=[nodes, payload, trigger_meta],
                countdown=interval
            )

        logging.info("Pipeline finished, nodes executed=%d", len(nodes))
        return {"status": "success", "results": results}
    except Exception as exc:
        logging.error("Pipeline exception: %s", traceback.format_exc())
        self.update_state(state="FAILURE", meta={"exc": str(exc)})
        raise

