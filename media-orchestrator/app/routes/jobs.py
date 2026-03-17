"""
Routes /jobs/*
"""

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.models import AdvanceJobRequest, FailJobRequest, JobStatus, JobSubmitRequest
from app.orchestrator import JobNotFoundError, OrchestratorError, MediaOrchestrator
from .deps import get_orchestrator
from typing import Annotated, Optional
from fastapi import Query, Depends
import json
import logging


router = APIRouter(prefix="/jobs", tags=["Jobs"])

logger = logging.getLogger(__name__)


@router.post("/submit", status_code=201)
async def submit_job(
    body: JobSubmitRequest,
    request: Request,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        #job_id    = f"job-{uuid.uuid4().hex[:12]}"
        job_id    = f"{uuid.uuid4().hex[:12]}"
        now       = datetime.now(timezone.utc).isoformat()
        job_input = body.model_dump()

        status = JobStatus(
            job_id        = job_id,
            status        = "pending",
            current_stage = "ingest",
            created_at    = now,
            updated_at    = now,
            metadata      = job_input,
            results       = {},
        )
        logger.info("Submitting job %s: %s", job_id, job_input)
        await orc.save_job(status)
        await orc.send_to_stage("ingest", {
            "job_id":        job_id,
            "created_at":    now,
            "input":         json.dumps(job_input),
            "current_stage": "ingest",
        })
        #  "input":         job_input,

        return {
            "success":    True,
            "job_id":     job_id,
            "message":    "Job submitted successfully",
            "status_url": f"/jobs/{job_id}/status",
        }

    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}")


@router.get("/list")
async def list_jobs(
    status: Annotated[Optional[str], Query(description="Filtrer: pending|processing|completed|failed")] = None,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        jobs = await orc.list_jobs(status_filter=status)
        return {"total": len(jobs), "jobs": jobs}
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@router.get("/{job_id}/status")
async def get_job_status(
    job_id: str,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        job = await orc.load_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return asdict(job)
    except HTTPException:
        raise
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@router.post("/{job_id}/next")
async def advance_job(
    job_id: str,
    body: AdvanceJobRequest,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        job = await orc.load_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        job.results[body.stage] = body.result
        job.updated_at = datetime.now(timezone.utc).isoformat()

        next_stage = orc.get_next_stage(body.stage)

        if next_stage and next_stage != "completed":
            await orc.send_to_stage(next_stage, {
                "job_id":           job_id,
                "input":            job.metadata,
                "current_stage":    next_stage,
                "previous_results": job.results,
                **body.result,
            })
            job.current_stage = next_stage
            job.status        = "processing"
            await orc.save_job(job)
        else:
            # Terminé → on supprime de la DB
            await orc.delete_job(job_id)
            return {
                "success":       True,
                "job_id":        job_id,
                "current_stage": "completed",
                "status":        "completed",
                "message":       "Job completed and removed from database",
            }

        return {
            "success":       True,
            "job_id":        job_id,
            "current_stage": job.current_stage,
            "status":        job.status,
        }

    except HTTPException:
        raise
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}")


@router.post("/{job_id}/failed")
async def mark_job_failed(
    job_id: str,
    body: FailJobRequest,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        job = await orc.load_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        job.status    = "failed"
        job.error     = f"Failed at stage '{body.stage}': {body.error}"
        job.updated_at = datetime.now(timezone.utc).isoformat()
        await orc.save_job(job)

        return {"success": True, "job_id": job_id, "status": "failed"}

    except HTTPException:
        raise
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}")


@router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    orc: MediaOrchestrator = Depends(get_orchestrator),
):
    try:
        job = await orc.load_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        await orc.delete_job(job_id)
        return {"success": True, "job_id": job_id, "message": "Job deleted"}
    except HTTPException:
        raise
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))