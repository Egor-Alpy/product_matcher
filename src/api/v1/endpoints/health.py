from fastapi import APIRouter, HTTPException


router = APIRouter(tags=["Health"])


@router.get("/healthz")
async def health_check():
    """Простой health check для Kubernetes liveness probe."""
    return {"status": "ok"}
