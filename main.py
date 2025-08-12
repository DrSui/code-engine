# main.py
import os
import uuid
import json
import datetime
from typing import List, Optional
from enum import Enum
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
import redis

from tasks import execute_pipeline

app = FastAPI(title="Trigger Pipeline API")

# Redis client (REDIS_URL configurable via env)
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# keys are "webhook:{token}"
WEBHOOK_KEY_PREFIX = "webhook:"
# PYDANTIC BS AI SLOP
class TriggerType(str, Enum):
    webhook = "webhook"
    time = "time"

class ScheduleMode(str, Enum):
    once = "once"
    interval = "interval"

class TimeSchedule(BaseModel):
    mode: ScheduleMode
    at: Optional[datetime.datetime] = None
    interval_seconds: Optional[int] = None

class TriggerNode(BaseModel):
    type: TriggerType
    schedule: Optional[TimeSchedule] = None

class Node(BaseModel):
    id: str
    logic: str
    params: dict = {}

class PipelineRegistration(BaseModel):
    flow_id: str = Field(..., description="External flow identifier to include in webhook URL")
    trigger: TriggerNode
    nodes: List[Node]

def redis_save_webhook(token: str, payload: dict):
    key = WEBHOOK_KEY_PREFIX + token
    redis_client.set(key, json.dumps(payload))

def redis_get_webhook(token: str) -> Optional[dict]:
    key = WEBHOOK_KEY_PREFIX + token
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None
def create_webhook_trigger(reg: PipelineRegistration, trigger_id):
        # create token and persist metadata to redis
        flow_id = reg.flow_id
        meta = {"trigger_id": trigger_id, "flow_id": flow_id, "nodes": [n.dict() for n in reg.nodes]}
        redis_save_webhook(trigger_id, meta)

        # return the stable URL; we use a static route so server restarts don't lose handlers
        webhook_url = f"/webhook/{flow_id}/{trigger_id}"
        return {"trigger_id": trigger_id, "webhook_url": webhook_url, "flow_id": flow_id}
async def create_interval_trigger(reg: PipelineRegistration, trigger_id):
    sched = reg.trigger.schedule
    if not sched:
        return HTTPException(status_code=400, detail="time trigger requires 'schedule'")

    nodes_to_pass = [n.dict() for n in reg.nodes]

    if not sched.interval_seconds or sched.interval_seconds <= 0:
        return HTTPException(status_code=400, detail="'interval_seconds' must be a positive integer")
    execute_pipeline.apply_async(
            args=[nodes_to_pass, {}, {"trigger_type": "time", "mode": "interval", "interval_seconds": sched.interval_seconds, "flow_id": reg.flow_id}],
            countdown=int(sched.interval_seconds)
            )
    return {"trigger_id": trigger_id, "mode": "interval", "interval_seconds": sched.interval_seconds}

@app.post("/triggers", summary="Register a trigger + nodes")
async def register_trigger(reg: PipelineRegistration):
    trigger_id = uuid.uuid4().hex

    if reg.trigger.type == TriggerType.webhook:
        output = create_webhook_trigger(reg,trigger_id)
    elif reg.trigger.type == TriggerType.time:
        output = await create_interval_trigger(reg, trigger_id)
    else:
        output = HTTPException(status_code=400, detail="Unknown trigger type")
    if isinstance(output,HTTPException) == True:
        raise output
    else:
        return output
# Single static webhook route that looks up token in Redis.
@app.post("/webhook/{flow_id}/{token}")
async def webhook_receiver(flow_id: str, token: str, request: Request):
    meta = redis_get_webhook(token)
    if not meta:
        raise HTTPException(status_code=404, detail="Webhook not found")

    # ensure flow_id matches stored flow_id
    if meta.get("flow_id") != flow_id:
        raise HTTPException(status_code=400, detail="flow_id mismatch")

    try:
        body = await request.json()
    except Exception:
        body = {}

    # enforce: user must not re-send nodes/trigger/flow_id during webhook invocation
    forbidden_keys = {"nodes", "trigger", "flow_id"}
    if isinstance(body, dict) and any(k in body for k in forbidden_keys):
        raise HTTPException(
            status_code=400,
            detail="Webhook POST must not include 'nodes', 'trigger', or 'flow_id'. Register pipeline once via /triggers."
        )

    payload = body if isinstance(body, dict) else {}

    # schedule celery task and pass nodes (persisted in redis) to the worker
    nodes = meta.get("nodes", [])
    execute_pipeline.apply_async(
        args=[nodes, payload, {"trigger_type": "webhook", "flow_id": meta.get("flow_id")}]
    )
    return {"status": "accepted", "trigger_id": meta.get("trigger_id"), "flow_id": meta.get("flow_id")}

