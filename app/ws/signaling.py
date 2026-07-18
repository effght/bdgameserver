import json
from dataclasses import dataclass
from typing import Dict, Optional, Set

from fastapi import WebSocket


@dataclass(eq=False)
class SignalingClient:
    websocket: WebSocket
    role: str = "unknown"
    room_id: str = "default"
    client_id: str = ""


class SignalingManager:
    def __init__(self) -> None:
        self.clients: Set[SignalingClient] = set()

    async def connect(self, websocket: WebSocket) -> SignalingClient:
        await websocket.accept()
        client = SignalingClient(websocket=websocket)
        self.clients.add(client)
        return client

    def disconnect(self, client: SignalingClient) -> None:
        self.clients.discard(client)

    async def send_json(self, client: SignalingClient, packet: Dict) -> None:
        await client.websocket.send_text(json.dumps(packet))

    async def route_message(self, message: str, packet: Dict, sender: Optional[SignalingClient] = None) -> None:
        sender_room_id = sender.room_id if sender else "default"
        room_id = str(packet.get("roomId") or packet.get("channelId") or sender_room_id)
        target_client_id = str(packet.get("targetClientId") or "")
        target_role = str(packet.get("targetRole") or "")
        dead = []

        for client in list(self.clients):
            if client is sender:
                continue
            if client.room_id != room_id:
                continue
            if target_client_id and client.client_id != target_client_id:
                continue
            if target_role and client.role != target_role:
                continue
            try:
                await client.websocket.send_text(message)
            except Exception:
                dead.append(client)

        for client in dead:
            self.disconnect(client)

    async def handle_text(self, client: SignalingClient, message: str) -> None:
        try:
            packet = json.loads(message)
        except json.JSONDecodeError:
            return

        if packet.get("type") == "hello":
            client.role = str(packet.get("role") or "unknown")
            client.room_id = str(packet.get("roomId") or packet.get("channelId") or "default")
            client.client_id = str(packet.get("clientId") or f"{client.role}:{id(client)}")
            await self.send_json(client, {
                "type": "hello_ack",
                "role": "bridge",
                "roomId": client.room_id,
                "clientId": client.client_id,
                "connected_clients": len(self.clients),
            })
            return

        packet.setdefault("roomId", client.room_id)
        packet.setdefault("sourceClientId", client.client_id)
        packet.setdefault("sourceRole", client.role)
        await self.route_message(json.dumps(packet), packet, sender=client)
