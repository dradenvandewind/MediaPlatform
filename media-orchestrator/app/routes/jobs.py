"""
Routes /jobs/*
"""

import json
import uuid
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..models import AdvanceJobRequest, FailJobRequest, JobStatus, JobSubmitRequest
from ..orchestrator import MediaOrchestrator
from .deps import get_orchestrator

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.post("/submit", status_code=201)
async def submit_job(
    body: JobSubmitRequest,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    now = datetime.utcnow().isoformat()
    job_input = body.model_dump()

    await orc.save_job_status(
        JobStatus(
            job_id=job_id,
            status="pending",
            current_stage="ingest",
            created_at=now,
            updated_at=now,
            metadata=job_input,
            results={},
        )
    )
    await orc.send_to_stage(
        "ingest",
        {
            "job_id": job_id,
            "created_at": now,
            "input": job_input,
            "current_stage": "ingest",
            "status": "pending",
        },
    )

    return {
        "success": True,
        "job_id": job_id,
        "message": "Job submitted successfully",
        "status_url": f"/jobs/{job_id}/status",
    }


@router.get("/list")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filtrer par statut"),
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    pattern = f"{orc.STATUS_KEY_PREFIX}:*"
    jobs = []
    cursor = 0

    while True:
        cursor, keys = await orc.redis.scan(cursor, match=pattern, count=100)
        for key in keys:
            raw = await orc.redis.get(key)
            if raw:
                jobs.append(json.loads(raw))
        if cursor == 0:
            break

    if status:
        jobs = [j for j in jobs if j.get("status") == status]

    jobs.sort(key=lambda x: x["created_at"], reverse=True)
    return {"total": len(jobs), "jobs": jobs}


@router.get("/{job_id}/status")
async def get_job_status(
    job_id: str,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    job = await orc.load_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return asdict(job)


@router.post("/{job_id}/next")
async def advance_job(
    job_id: str,
    body: AdvanceJobRequest,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    job = await orc.load_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job.results[body.stage] = body.result
    job.updated_at = datetime.utcnow().isoformat()

    next_stage = orc.get_next_stage(body.stage)

    if next_stage:
        await orc.send_to_stage(
            next_stage,
            {
                "job_id": job_id,
                "input": job.metadata,
                "current_stage": next_stage,
                "previous_results": job.results,
                **body.result,
            },
        )
        job.current_stage = next_stage
        job.status = "processing"
    else:
        job.status = "completed"
        job.current_stage = "completed"

    await orc.save_job_status(job)
    return {
        "success": True,
        "job_id": job_id,
        "current_stage": job.current_stage,
        "status": job.status,
    }


@router.post("/{job_id}/failed")
async def mark_job_failed(
    job_id: str,
    body: FailJobRequest,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    job = await orc.load_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job.status = "failed"
    job.error = f"Failed at stage '{body.stage}': {body.error}"
    job.updated_at = datetime.utcnow().isoformat()
    await orc.save_job_status(job)

    return {"success": True, "job_id": job_id, "status": "failed"}
