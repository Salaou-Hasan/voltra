#!/usr/bin/env python3
"""Voltra Python client SDK.

Requires:
    pip install websocket-client msgpack
"""

import json
import msgpack
import websocket
from typing import Any, Callable, Dict, Optional


class VoltraClient:
    def __init__(self, url: str = "ws://localhost:3000", api_key: Optional[str] = None):
        self.url = url
        self.api_key = api_key
        self.ws = None
        self.call_id = 0

    def connect(self):
        headers = []
        if self.api_key:
            headers.append(f"Authorization: Bearer {self.api_key}")
        self.ws = websocket.create_connection(self.url, header=headers)

    def disconnect(self):
        if self.ws:
            self.ws.close()
            self.ws = None

    def _send_message(self, message: Dict[str, Any]):
        if self.ws is None:
            raise RuntimeError("WebSocket is not connected")
        data = msgpack.packb(message)
        self.ws.send_binary(data)

    def _recv_message(self) -> Any:
        raw = self.ws.recv()
        if isinstance(raw, bytes):
            return msgpack.unpackb(raw, raw=False)
        return json.loads(raw)

    def increment(self, name: str, delta: int) -> Dict[str, Any]:
        self.call_id += 1
        args = msgpack.packb({"name": name, "delta": delta})
        call = {
            "call_id": self.call_id,
            "reducer_name": "increment",
            "args": args,
        }
        self._send_message(call)
        return self._recv_message()

    def subscribe(self, subscription_id: str, query: str) -> Dict[str, Any]:
        message = {
            "type": "Subscribe",
            "payload": {
                "subscription_id": subscription_id,
                "query": query,
            },
        }
        self._send_message(message)
        return self._recv_message()

    def unsubscribe(self, subscription_id: str) -> Dict[str, Any]:
        message = {
            "type": "Unsubscribe",
            "payload": {
                "subscription_id": subscription_id,
            },
        }
        self._send_message(message)
        return self._recv_message()

    def receive_diff(self) -> Dict[str, Any]:
        return self._recv_message()


if __name__ == "__main__":
    print("This module provides a Voltra Python client SDK.")
