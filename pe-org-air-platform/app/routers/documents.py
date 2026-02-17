from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from app.services.evidence_store import EvidenceStore
router = APIRouter(prefix="/documents")

@router.get("")
def list_documents(
   ticker: str | None = Query(default=None),
   company_id: str | None = Query(default=None),
   limit: int = Query(default=100, ge=1, le=500),
):
   store = EvidenceStore()
   try:
       return store.list_documents(ticker=ticker, company_id=company_id, limit=limit)
   finally:
       store.close()

@router.get("/{document_id}")
def get_document(document_id: str):
   store = EvidenceStore()
   try:
       doc = store.get_document(document_id)
       if not doc:
           raise HTTPException(status_code=404, detail="Document not found")
       return doc
   finally:
       store.close()