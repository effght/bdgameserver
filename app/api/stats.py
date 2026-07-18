from fastapi import APIRouter

router = APIRouter(prefix="/stats", tags=["stats"])


@router.post("")
async def receive_stats(payload: dict):
    # Placeholder for future match/rally persistence.
    return {"ok": True, "received": payload}
