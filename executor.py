# executor.py
import os
import json
import tempfile
import subprocess
import sys
import traceback
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import resource  # unix only; try/except if not available
from typing import Any, Optional

app = FastAPI(title="Code Executor")

# Tunables via env
DEFAULT_TIMEOUT = int(os.environ.get("CUSTOM_TIMEOUT_SECONDS", "5"))
DEFAULT_CPU = int(os.environ.get("CUSTOM_CPU_SECONDS", "2"))
DEFAULT_MEM_MB = int(os.environ.get("CUSTOM_MEMORY_MB", "100"))


class ExecRequest(BaseModel):
    code: str
    prev: Optional[Any] = None
    params: Optional[dict] = {}
    payload: Optional[dict] = {}
    timeout_seconds: Optional[int] = None


def _set_limits():
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (DEFAULT_CPU, DEFAULT_CPU))
        mem_bytes = DEFAULT_MEM_MB * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))
    except Exception:
        pass


@app.post("/run")
def run_code(req: ExecRequest):
    code = req.code
    timeout = req.timeout_seconds or DEFAULT_TIMEOUT

    if not code or not isinstance(code, str) or not code.strip():
        raise HTTPException(status_code=400, detail="empty code")

    try:
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tf:
            tmp_path = tf.name
            # write imports and JSON-decoded variables
            tf.write("import json\n")
            # pass prev/params/payload as JSON literals so user can access them
            tf.write(f"prev = json.loads({json.dumps(json.dumps(req.prev))})\n")
            tf.write(f"params = json.loads({json.dumps(json.dumps(req.params))})\n")
            tf.write(f"payload = json.loads({json.dumps(json.dumps(req.payload))})\n")
            tf.write("\n# --- user code starts ---\n")
            tf.write(code)
            tf.write("\n# --- user code ends ---\n")

        cmd = [sys.executable, tmp_path]
        try:
            completed = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                preexec_fn=_set_limits
            )
        except TypeError:
            # fallback if preexec_fn unsupported
            completed = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout
            )
        except subprocess.TimeoutExpired as te:
            return {
                "ok": False,
                "error": "timeout",
                "timeout_seconds": timeout,
                "stdout": te.stdout,
                "stderr": te.stderr
            }

        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        returncode = completed.returncode

        parsed = None
        try:
            parsed = json.loads(stdout)
        except Exception:
            parsed = None

        return {
            "ok": True if returncode == 0 else False,
            "returncode": returncode,
            "stdout": parsed if parsed is not None else stdout,
            "stderr": stderr,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}
    finally:
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass

