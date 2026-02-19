from typing import List, Optional

from fastapi import APIRouter, Body, HTTPException

from app.services.scoring_service import compute_for_companies

router = APIRouter(prefix="/scoring", tags=["scoring"])

@router.post("/compute/{company_id}")
def compute_single_company(company_id: str):
    run_id = compute_for_companies([company_id])
    return {"status": "submitted", "run_id": run_id}

@router.post("/compute")
def compute_batch(
    company_ids: Optional[List[str]] = Body(default=None),
):
    if not company_ids:
        raise HTTPException(status_code=400, detail="company_ids required")

    run_id = compute_for_companies(company_ids)
    return {"status": "submitted", "run_id": run_id}
