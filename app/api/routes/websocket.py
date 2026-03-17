"""WebSocket endpoint for real-time data push (P2 Issue: WebSocket Real-time)."""
import json
import asyncio
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["WebSocket"])


class ConnectionManager:
    """Manages active WebSocket connections grouped by channel."""

    def __init__(self):
        self._connections: dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, channel: str):
        await websocket.accept()
        if channel not in self._connections:
            self._connections[channel] = set()
        self._connections[channel].add(websocket)

    def disconnect(self, websocket: WebSocket, channel: str):
        if channel in self._connections:
            self._connections[channel].discard(websocket)
            if not self._connections[channel]:
                del self._connections[channel]

    async def broadcast(self, channel: str, message: dict):
        if channel not in self._connections:
            return
        dead = []
        for ws in self._connections[channel]:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections[channel].discard(ws)

    @property
    def active_count(self) -> int:
        return sum(len(s) for s in self._connections.values())


manager = ConnectionManager()


@router.websocket("/ws/{channel}")
async def websocket_endpoint(websocket: WebSocket, channel: str):
    """
    WebSocket endpoint for real-time data push.

    Channels:
    - market:{symbol}  — real-time market quotes
    - alerts:{user_id} — alert notifications
    - orders:{user_id} — order status updates
    - portfolio:{user_id} — P&L updates
    """
    await manager.connect(websocket, channel)
    try:
        while True:
            # Keep connection alive; handle incoming messages
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON"})
                continue

            msg_type = msg.get("type")
            if msg_type == "ping":
                await websocket.send_json({"type": "pong"})
            elif msg_type == "subscribe":
                new_channel = msg.get("channel")
                if new_channel:
                    await manager.connect(websocket, new_channel)
                    await websocket.send_json({"type": "subscribed", "channel": new_channel})
            elif msg_type == "unsubscribe":
                old_channel = msg.get("channel")
                if old_channel:
                    manager.disconnect(websocket, old_channel)
                    await websocket.send_json({"type": "unsubscribed", "channel": old_channel})
    except WebSocketDisconnect:
        manager.disconnect(websocket, channel)
