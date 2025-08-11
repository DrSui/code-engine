# main.py
import uuid
from typing import List, Optional
from enum import Enum
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
import datetime

from tasks import execute_pipeline

app = FastAPI(title="Trigger Pipeline API")

# --- Pydantic models --------------------------------------------------------
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
    # NEW: flow_id is required and will be embedded in the webhook URL
    flow_id: str = Field(..., description="External flow identifier to include in webhook URL")
    trigger: TriggerNode
    nodes: List[Node]


# --- in-memory mapping so we can report webhook URLs created
# NOTE: in production you should persist this to a database
# token -> metadata (trigger_id, flow_id, nodes)
WEBHOOK_REGISTRY: dict = {}
# ----------------------------------------------------------------------------


@app.post("/triggers", summary="Register a trigger + nodes")
async def register_trigger(reg: PipelineRegistration):
    """
    Register a trigger and its pipeline of nodes.

    - For webhook triggers: returns a unique webhook URL that contains the provided flow_id.
      When that URL receives a POST, the pipeline will be executed with the incoming JSON payload.
    - For time triggers: schedules a Celery background job (one-off or recurring interval).
    """
    trigger_id = uuid.uuid4().hex

    if reg.trigger.type == TriggerType.webhook:
        # create a unique token and dynamic endpoint that includes the provided flow_id
        token = uuid.uuid4().hex
        # ensure flow_id is URL-safe in your environment or validate earlier
        flow_id = reg.flow_id
        path = f"/webhook/{flow_id}/{token}"

        # store nodes and flow_id
        WEBHOOK_REGISTRY[token] = {
            "trigger_id": trigger_id,
            "flow_id": flow_id,
            "nodes": [n.dict() for n in reg.nodes],
        }

        async def webhook_handler(request: Request, _token=token):
            # confirm token exists (simple guard)
            meta = WEBHOOK_REGISTRY.get(_token)
            if not meta:
                raise HTTPException(status_code=404, detail="Webhook not found")

            try:
                payload = await request.json()
            except Exception:
                payload = {}

            # pass incoming payload to the same Celery task as before
            execute_pipeline.apply_async(
                args=[meta["nodes"], payload, {"trigger_type": "webhook", "flow_id": meta["flow_id"]}]
            )
            return {"status": "accepted", "trigger_id": meta["trigger_id"], "flow_id": meta["flow_id"]}

        # register route dynamically if not already added
        existing = [r.path for r in app.routes]
        if path not in existing:
            app.router.add_api_route(path, webhook_handler, methods=["POST"])

        # return path (relative). In production return full URL including scheme/domain.
        webhook_url = path
        return {"trigger_id": trigger_id, "webhook_url": webhook_url, "flow_id": flow_id}

    elif reg.trigger.type == TriggerType.time:
        sched = reg.trigger.schedule
        if not sched:
            raise HTTPException(status_code=400, detail="time trigger requires 'schedule'")

        nodes_to_pass = [n.dict() for n in reg.nodes]

        if sched.mode == ScheduleMode.once:
            if not sched.at:
                raise HTTPException(status_code=400, detail="'at' datetime required for mode 'once'")
            now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            at = sched.at
            if at.tzinfo is None:
                at = at.replace(tzinfo=datetime.timezone.utc)
            delay = (at - now).total_seconds()
            if delay < 0:
                raise HTTPException(status_code=400, detail="'at' must be in the future")
            execute_pipeline.apply_async(
                args=[nodes_to_pass, {}, {"trigger_type": "time", "mode": "once", "flow_id": reg.flow_id}],
                countdown=int(delay)
            )
            return {"trigger_id": trigger_id, "scheduled_in_seconds": int(delay), "mode": "once", "at": sched.at.isoformat()}

        elif sched.mode == ScheduleMode.interval:
            if not sched.interval_seconds or sched.interval_seconds <= 0:
                raise HTTPException(status_code=400, detail="'interval_seconds' must be a positive integer")
            execute_pipeline.apply_async(
                args=[nodes_to_pass, {}, {"trigger_type": "time", "mode": "interval", "interval_seconds": sched.interval_seconds, "flow_id": reg.flow_id}],
                countdown=int(sched.interval_seconds)
            )
            return {"trigger_id": trigger_id, "mode": "interval", "interval_seconds": sched.interval_seconds}

    else:
        raise HTTPException(status_code=400, detail="Unknown trigger type")

