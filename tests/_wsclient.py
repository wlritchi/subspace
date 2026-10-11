# vim: filetype=python
"""A small WebSocket client for end-to-end tests against a spawned server."""

import asyncio
import json
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

import subspace_protocol as proto


class Client:
    """One connection. Replies are matched by req_id; notify frames queue up."""

    def __init__(self, ws: ClientConnection) -> None:
        self.ws: ClientConnection = ws
        self._next_req: int = 0
        self._replies: dict[int, asyncio.Future[dict]] = {}
        self.notifies: asyncio.Queue[dict] = asyncio.Queue()
        self.errors: list[dict] = []
        self._task: asyncio.Task[None] = asyncio.create_task(self._recv())

    @classmethod
    async def open(cls, port: int, token: str) -> "Client":
        ws = await connect(
            f"ws://127.0.0.1:{port}",
            additional_headers={proto.AUTH_HEADER: proto.auth_header_value(token)},
        )
        return cls(ws)

    async def _recv(self) -> None:
        try:
            async for raw in self.ws:
                frame = json.loads(raw)
                req_id = frame.get("req_id")
                if frame.get("type") == proto.NOTIFY:
                    await self.notifies.put(frame)
                elif req_id is not None and req_id in self._replies:
                    self._replies.pop(req_id).set_result(frame)
                elif frame.get("type") == proto.ERROR:
                    self.errors.append(frame)
        except ConnectionClosed as exc:
            # Fail the requests that wait for a reply, so they raise at once.
            for future in self._replies.values():
                if not future.done():
                    future.set_exception(exc)
            self._replies.clear()

    async def send(self, frame: dict[str, Any]) -> None:
        await self.ws.send(json.dumps(frame))

    async def request(self, frame: dict[str, Any], timeout: float = 5.0) -> dict:
        self._next_req += 1
        req_id = self._next_req
        future: asyncio.Future[dict] = asyncio.get_running_loop().create_future()
        self._replies[req_id] = future
        try:
            await self.send({**frame, "req_id": req_id})
        except ConnectionClosed:
            self._replies.pop(req_id, None)
            future.cancel()
            raise
        return await asyncio.wait_for(future, timeout)

    async def register(self, session_id: str) -> None:
        await self.send({"type": proto.REGISTER, "session_id": session_id})
        # register has no reply; a whoami round trip orders it.
        await self.request({"type": proto.WHOAMI})

    async def notify(self, timeout: float = 5.0) -> dict:
        return await asyncio.wait_for(self.notifies.get(), timeout)

    async def notify_of_kind(self, kind: str, timeout: float = 5.0) -> dict:
        """The next notify whose meta kind is ``kind``, skipping others."""
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            frame = await self.notify(max(0.01, remaining))
            if frame["meta"].get("kind") == kind:
                return frame

    async def ack(self, frame: dict) -> None:
        await self.send({"type": proto.ACK, "id": frame["id"]})

    async def close(self) -> None:
        await self.ws.close()
        self._task.cancel()
