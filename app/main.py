from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.rooms import router as rooms_router
from app.api.stats import router as stats_router
from app.api.users import router as users_router
from app.ws.signaling import SignalingManager


BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Badminton Simulator Server")
signaling = SignalingManager()

app.include_router(rooms_router, prefix="/api")
app.include_router(stats_router, prefix="/api")
app.include_router(users_router, prefix="/api")
app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")


@app.get("/")
async def index_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/index.html")
async def index_file():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/player")
async def player_page():
    return FileResponse(STATIC_DIR / "bd_v3_player.html")


@app.get("/hit")
async def hit_controller_page():
    return FileResponse(STATIC_DIR / "hit_webrtc.html")


@app.get("/hit_webrtc.html")
async def hit_controller_file():
    return FileResponse(STATIC_DIR / "hit_webrtc.html")


@app.get("/bd_v3_player.html")
async def player_file():
    return FileResponse(STATIC_DIR / "bd_v3_player.html")


@app.websocket("/ws")
async def websocket_bridge(websocket: WebSocket):
    client = await signaling.connect(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            await signaling.handle_text(client, message)
    except WebSocketDisconnect:
        signaling.disconnect(client)
    except Exception:
        signaling.disconnect(client)
        raise
