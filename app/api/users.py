from fastapi import APIRouter

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me")
async def get_current_user_placeholder():
    # Authentication can be added later without changing the static game files.
    return {"anonymous": True}
