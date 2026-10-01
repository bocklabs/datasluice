from __future__ import annotations

import json
from typing import Any

from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

_JSON_HEADERS = {"Content-Type": "application/json"}


def success_body(result: Any) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def failure_body(error: Any) -> bytes:
    return json.dumps({"success": False, "error": error}).encode("utf-8")


class SyncCaptureTransport:
    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(status_code=self.status_code, headers=dict(_JSON_HEADERS), body=self.body)

    def close(self) -> None:
        self.close_count += 1


class AsyncCaptureTransport:
    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(status_code=self.status_code, headers=dict(_JSON_HEADERS), body=self.body)

    async def aclose(self) -> None:
        self.close_count += 1
