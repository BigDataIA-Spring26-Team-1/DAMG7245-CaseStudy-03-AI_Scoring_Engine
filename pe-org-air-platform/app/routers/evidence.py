# app/routers/evidence.py
from __future__ import annotations
from fastapi import APIRouter, Query

from app.routers.documents import get_document as get_document_from_documents_router
from app.routers.documents import list_documents as list_documents_from_documents_router
from app.services.evidence_store import EvidenceStore

router = APIRouter(prefix="/evidence")

@router.get("/stats")
def stats():
   store = EvidenceStore()
   try:
       return store.stats()
   finally:
       store.close()

@router.get("/documents")
def list_documents(
   ticker: str | None = Query(default=None),
   company_id: str | None = Query(default=None),
   limit: int = Query(default=100, ge=1, le=500),
   offset: int = Query(default=0, ge=0),
):
   # Backward-compatible alias of /documents endpoint.
   return list_documents_from_documents_router(ticker=ticker, company_id=company_id, limit=limit, offset=offset)

@router.get("/documents/{document_id}")
def get_document(document_id: str):
   # Backward-compatible alias of /documents/{document_id} endpoint.
   return get_document_from_documents_router(document_id=document_id)

@router.get("/documents/{document_id}/chunks")
def get_chunks(
   document_id: str,
   limit: int = Query(default=200, ge=1, le=1000),
   offset: int = Query(default=0, ge=0),
):
   store = EvidenceStore()
   try:
       return store.list_chunks(document_id=document_id, limit=limit, offset=offset)
   finally:
       store.close()
