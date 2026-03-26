"""WebSocket endpoint for real-time data push (P2 Issue: WebSocket Real-time)."""

import json
from typing import Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, status

from app.api.services.auth_service import decode_token

router = APIRouter(tags=["WebSocket"])

# Channels that contain user-specific data and require ownership validation
_USER_CHANNEL_PREFIXES = ("alerts:", "orders:", "portfolio:", "paper-signals:", "paper-orders:")


def _validate_channel_access(channel: str, user_id: int) -> bool:
    """Check that user-specific channels match the authenticated user."""
    for prefix in _USER_CHANNEL_PREFIXES:
        if channel.startswith(prefix):
            try:
                return int(channel[len(prefix) :]) == user_id
            except (ValueError, IndexError):
                return False
    return True  # public channels (e.g. market:) are open


class ConnectionManager:
    """Manages active WebSocket connections grouped by channel."""

    def __init__(self):
        self._connections: dict[str, Set[WebSocket]] = {}
        self._subscriptions: dict[int, set[str]] = {}  # ws id → set of channels

    async def connect(self, websocket: WebSocket, channel: str):
        if channel not in self._connections:
            self._connections[channel] = set()
        self._connections[channel].add(websocket)
        ws_id = id(websocket)
        if ws_id not in self._subscriptions:
            self._subscriptions[ws_id] = set()
        self._subscriptions[ws_id].add(channel)

    def disconnect_all(self, websocket: WebSocket):
        """Remove a connection from ALL subscribed channels."""
        ws_id = id(websocket)
        channels = self._subscriptions.pop(ws_id, set())
        for ch in channels:
            if ch in self._connections:
                self._connections[ch].discard(websocket)
                if not self._connections[ch]:
                    del self._connections[ch]

    def disconnect(self, websocket: WebSocket, channel: str):
        ws_id = id(websocket)
        if channel in self._connections:
            self._connections[channel].discard(websocket)
            if not self._connections[channel]:
                del self._connections[channel]
        if ws_id in self._subscriptions:
            self._subscriptions[ws_id].discard(channel)

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
async def websocket_endpoint(websocket: WebSocket, channel: str, token: str = Query(...)):
    """
    WebSocket endpoint for real-time data push.
    Requires a valid JWT token as a query parameter.

    Channels:
    - market:{symbol}  — real-time market quotes
    - alerts:{user_id} — alert notifications
    - orders:{user_id} — order status updates
    - portfolio:{user_id} — P&L updates
    """
    # Authenticate via token query parameter
    token_data = decode_token(token)
    if not token_data:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # Validate channel access
    if not _validate_channel_access(channel, token_data.user_id):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    await manager.connect(websocket, channel)
    try:
        while True:
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
                if new_channel and _validate_channel_access(new_channel, token_data.user_id):
                    await manager.connect(websocket, new_channel)
                    await websocket.send_json({"type": "subscribed", "channel": new_channel})
                else:
                    await websocket.send_json({"type": "error", "message": "Channel access denied"})
            elif msg_type == "unsubscribe":
                old_channel = msg.get("channel")
                if old_channel:
                    manager.disconnect(websocket, old_channel)
                    await websocket.send_json({"type": "unsubscribed", "channel": old_channel})
    except WebSocketDisconnect:
        manager.disconnect_all(websocket)
