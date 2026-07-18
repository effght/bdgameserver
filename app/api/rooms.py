from uuid import uuid4

from fastapi import APIRouter

router = APIRouter(prefix="/rooms", tags=["rooms"])


@router.post("")
async def create_room():
    return {"roomId": str(uuid4())}
