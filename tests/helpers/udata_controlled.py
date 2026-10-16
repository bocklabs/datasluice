from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import select
import signal
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING, cast
from urllib.parse import SplitResult, urlsplit

from datasluice.connectors.catalog.udata.clients import (
    _CONTROLLED_COMPOSE_SHA256,
    _CONTROLLED_DEPENDENCY_IMAGE_SPECS,
    _CONTROLLED_DOCKERFILE_SHA256,
    _CONTROLLED_ORIGIN,
    _CONTROLLED_SOURCE_COMMIT,
    _JSON_MEDIA_TYPE,
    AsyncUDataClient,
    SyncUDataClient,
    _build_controlled_async_transport,
    _build_controlled_sync_transport,
    _controlled_async_registry,
    _controlled_dispatch_gate,
    _controlled_error,
    _controlled_sync_registry,
    _ControlledAsyncOperations,
    _ControlledSourceIdentity,
    _ControlledStackEvidence,
    _ControlledSyncOperations,
    _ControlledVerification,
    _response_header,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.settlement import SETTLEMENT_ERRORS
from datasluice.domain.catalog.auth import SecretValue, UDataCredential
from datasluice.errors.catalog import (
    CatalogValidationError,
)
from datasluice.runtime.extras import require_extra
from datasluice.runtime.transport.base import (
    CatalogTransport,
    RedirectPolicy,
    RuntimeRequest,
    RuntimeResponse,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.settings import UDataClientSettings
    from datasluice.runtime.clients import AsyncCatalogTransport

_CONTROLLED_UDATA_LOCAL_DOCKER_CONTEXT = "Controlled uData evidence requires a local Unix Docker context."
_CONTROLLED_STACK_IDENTITY_TIMEOUT = "The controlled uData stack identity check timed out."
_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT = "The controlled uData stack identity check returned too much output."
_CONTROLLED_STACK_IDENTITY_FAILED = "The controlled uData stack identity check failed."
_CONTROLLED_SITE_PATCH_TOO_MUCH_OUTPUT = "The controlled uData site PATCH returned too much output."
_CONTROLLED_SITE_PATCH_INVALID_OUTPUT = "The controlled uData site PATCH returned invalid output."
_CONTROLLED_SERVICE_IMAGE_NOT_APPROVED = "The controlled uData service image is not the approved build."

_CONTROLLED_UDATA_IMAGE_REPOSITORY = "udata-evidence-udata"
_CONTROLLED_UDATA_VERSION = "17.6.0"

_CONTROLLED_UDATA_IMAGE_SPEC = ("udata-evidence-udata", "", "udata-evidence-udata")
_CONTROLLED_SERVICE_NAMES = ("udata", "mongo", "redis", "search", "storage", "mailpit")

_CONTROLLED_EVIDENCE_ROOT = Path(__file__).resolve().parents[2] / "dev" / "udata-evidence"
_CONTROLLED_COMPOSE_FILE = _CONTROLLED_EVIDENCE_ROOT / "compose.yaml"
_CONTROLLED_ENV_FILE = _CONTROLLED_EVIDENCE_ROOT / ".env"

_CONTROLLED_COMMAND_TIMEOUT_SECONDS = 15.0
_CONTROLLED_REAP_TIMEOUT_SECONDS = 1.0
_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES = 65536

_CONTROLLED_DOCKER_EXECUTABLES = (
    Path("/usr/local/bin/docker"),
    Path("/opt/homebrew/bin/docker"),
    Path("/usr/bin/docker"),
)

_CONTROLLED_PATCH_PROGRAM = """
import json
import sys
import urllib.error
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def emit(response):
    body = response.read(8193)
    print(json.dumps({
        "status": getattr(response, "status", getattr(response, "code", 0)),
        "content_type": response.headers.get("Content-Type", ""),
        "location": response.headers.get("Location", ""),
        "body": body.decode("utf-8", "replace"),
    }, separators=(",", ":")))

request_data = json.loads(sys.stdin.read())
request = urllib.request.Request(
    "http://127.0.0.1:7000/api/1/site/",
    data=json.dumps(request_data["body"], separators=(",", ":")).encode(),
    headers={"Content-Type": "application/json", "X-API-KEY": request_data["token"]},
    method="PATCH",
)
try:
    response = urllib.request.build_opener(NoRedirect()).open(request, timeout=10)
except urllib.error.HTTPError as error:
    emit(error)
else:
    try:
        emit(response)
    finally:
        response.close()
""".strip()
_CONTROLLED_SITE_PROBE_PROGRAM = """
import json
import urllib.error
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

request = urllib.request.Request("http://127.0.0.1:7000/api/1/site/")
try:
    response = urllib.request.build_opener(NoRedirect()).open(request, timeout=10)
except urllib.error.HTTPError as error:
    response = error
try:
    body = response.read(8193)
    print(json.dumps({
        "status": getattr(response, "status", getattr(response, "code", 0)),
        "content_type": response.headers.get("Content-Type", ""),
        "location": response.headers.get("Location", ""),
        "body": body.decode("utf-8", "replace"),
    }, separators=(",", ":")))
finally:
    response.close()
""".strip()


@dataclass(frozen=True, slots=True)
class _ControlledCommandSpecBinding:
    compose_file: Path
    env_file: Path
    compose_file_exists: Callable[[], bool]
    env_file_exists: Callable[[], bool]
    executable_candidates: tuple[tuple[str, Callable[[], bool], Callable[[str, int], bool]], ...]
    environment_get: Callable[[str], str | None]
    url_split: Callable[[str], SplitResult]
    error: Callable[[str], CatalogValidationError]
    input_limit: int
    executable_flag: int
    path_value: str


def _controlled_command_spec_binding() -> _ControlledCommandSpecBinding:
    compose_file = _CONTROLLED_COMPOSE_FILE
    env_file = _CONTROLLED_ENV_FILE
    environment_get = os.environ.get
    return _ControlledCommandSpecBinding(
        compose_file=compose_file,
        env_file=env_file,
        compose_file_exists=compose_file.is_file,
        env_file_exists=env_file.is_file,
        executable_candidates=tuple((str(path), path.is_file, os.access) for path in _CONTROLLED_DOCKER_EXECUTABLES),
        environment_get=environment_get,
        url_split=urlsplit,
        error=_controlled_error,
        input_limit=_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES,
        executable_flag=os.X_OK,
        path_value=environment_get("PATH", "/usr/bin:/bin"),
    )


def _validate_bound_command_input(
    binding: _ControlledCommandSpecBinding, input_data: bytes | None, docker_endpoint: str | None
) -> None:
    if not binding.compose_file_exists() or not binding.env_file_exists():
        raise binding.error("The controlled uData evidence configuration is unavailable.")
    if input_data is not None and len(input_data) > binding.input_limit:
        raise binding.error("The controlled uData command input is too large.")
    if docker_endpoint is not None:
        endpoint = binding.url_split(docker_endpoint)
        if (
            endpoint.scheme != "unix"
            or endpoint.netloc
            or not endpoint.path.startswith("/")
            or endpoint.query
            or endpoint.fragment
        ):
            raise binding.error(_CONTROLLED_UDATA_LOCAL_DOCKER_CONTEXT)


def _bound_docker_executable(binding: _ControlledCommandSpecBinding) -> str:
    executable = next(
        (
            path
            for path, is_file, access in binding.executable_candidates
            if is_file() and access(path, binding.executable_flag)
        ),
        None,
    )
    if executable is None:
        raise binding.error("The trusted Docker executable is unavailable.")
    return executable


def _bound_command(
    binding: _ControlledCommandSpecBinding,
    executable: str,
    args: tuple[str, ...],
    docker_endpoint: str | None,
    direct: bool,
) -> tuple[tuple[str, ...], dict[str, str]]:
    prefix = (executable,) if docker_endpoint is None else (executable, "--host", docker_endpoint)
    if direct or args[:2] == ("context", "inspect"):
        command = (*prefix, *args)
    else:
        command = (
            *prefix,
            "compose",
            "--env-file",
            str(binding.env_file),
            "-f",
            str(binding.compose_file),
            *args,
        )
    return command, {"PATH": binding.path_value}


def _call_bound_command_spec(
    binding: _ControlledCommandSpecBinding,
    args: tuple[str, ...],
    input_data: bytes | None,
    docker_endpoint: str | None,
    direct: bool,
) -> tuple[tuple[str, ...], dict[str, str]]:
    _validate_bound_command_input(binding, input_data, docker_endpoint)
    executable = _bound_docker_executable(binding)
    if binding.environment_get("DOCKER_HOST") or binding.environment_get("DOCKER_CONTEXT"):
        raise binding.error("Controlled uData evidence cannot use Docker environment overrides.")
    return _bound_command(binding, executable, args, docker_endpoint, direct)


def _controlled_command_spec(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
) -> tuple[tuple[str, ...], dict[str, str]]:
    return _call_bound_command_spec(_controlled_command_spec_binding(), args, input_data, docker_endpoint, direct)


def _make_bound_controlled_command_spec() -> Callable[..., tuple[tuple[str, ...], dict[str, str]]]:
    binding = _controlled_command_spec_binding()

    def command_spec(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
    ) -> tuple[tuple[str, ...], dict[str, str]]:
        return _call_bound_command_spec(binding, args, input_data, docker_endpoint, direct)

    return command_spec


@dataclass(frozen=True, slots=True)
class _ControlledSyncRuntime:
    command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]]
    error: Callable[[str], CatalogValidationError]
    pipe: Callable[[], tuple[int, int]]
    posix_spawnp: Callable[..., int]
    close: Callable[[int], None]
    set_blocking: Callable[[int, bool], None]
    read: Callable[[int, int], bytes]
    write: Callable[[int, bytes], int]
    kill: Callable[[int, int], None]
    waitpid: Callable[..., tuple[int, int]]
    waitstatus_to_exitcode: Callable[[int], int]
    select: Callable[..., tuple[list[int], list[int], list[int]]]
    monotonic: Callable[[], float]
    posix_spawn_dup2: int
    posix_spawn_close: int
    wnohang: int
    sigkill: int
    timeout_seconds: float
    reap_timeout_seconds: float
    max_output_bytes: int


def _current_controlled_sync_runtime(
    *, command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]] | None = None
) -> _ControlledSyncRuntime:
    return _ControlledSyncRuntime(
        command_spec=_controlled_command_spec if command_spec is None else command_spec,
        error=_controlled_error,
        pipe=os.pipe,
        posix_spawnp=os.posix_spawnp,
        close=os.close,
        set_blocking=os.set_blocking,
        read=os.read,
        write=os.write,
        kill=os.kill,
        waitpid=os.waitpid,
        waitstatus_to_exitcode=os.waitstatus_to_exitcode,
        select=select.select,
        monotonic=monotonic,
        posix_spawn_dup2=os.POSIX_SPAWN_DUP2,
        posix_spawn_close=os.POSIX_SPAWN_CLOSE,
        wnohang=os.WNOHANG,
        sigkill=signal.SIGKILL,
        timeout_seconds=_CONTROLLED_COMMAND_TIMEOUT_SECONDS,
        reap_timeout_seconds=_CONTROLLED_REAP_TIMEOUT_SECONDS,
        max_output_bytes=_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES,
    )


def _terminate_controlled_process_sync(process_id: int, runtime: _ControlledSyncRuntime) -> None:
    with contextlib.suppress(OSError):
        runtime.kill(process_id, runtime.sigkill)
    reap_deadline = runtime.monotonic() + runtime.reap_timeout_seconds
    while True:
        try:
            completed_pid, _ = runtime.waitpid(process_id, runtime.wnohang)
        except OSError:
            return
        if completed_pid == process_id:
            return
        remaining = reap_deadline - runtime.monotonic()
        if remaining <= 0:
            return
        runtime.select([], [], [], min(remaining, 0.05))


def _close_controlled_descriptor(fd: int, runtime: _ControlledSyncRuntime) -> Exception | None:
    if fd < 0:
        return None
    try:
        runtime.close(fd)
    except Exception as error:
        return error
    return None


def _close_controlled_descriptors(descriptors: tuple[int, ...], runtime: _ControlledSyncRuntime) -> list[Exception]:
    errors = []
    for fd in descriptors:
        error = _close_controlled_descriptor(fd, runtime)
        if error is not None:
            errors.append(error)
    return errors


def _write_controlled_input(
    process_id: int,
    fd: int,
    input_data: bytes,
    deadline: float,
    timeout_message: str,
    runtime: _ControlledSyncRuntime,
) -> None:
    runtime.set_blocking(fd, False)
    offset = 0
    while offset < len(input_data):
        remaining = deadline - runtime.monotonic()
        if remaining <= 0 or not runtime.select([], [fd], [], remaining)[1]:
            _terminate_controlled_process_sync(process_id, runtime)
            raise runtime.error(timeout_message)
        try:
            offset += runtime.write(fd, input_data[offset:])
        except BlockingIOError:
            continue


def _read_controlled_output(
    process_id: int,
    fd: int,
    deadline: float,
    timeout_message: str,
    output_message: str,
    runtime: _ControlledSyncRuntime,
) -> list[bytes]:
    chunks: list[bytes] = []
    output_size = 0
    while True:
        remaining = deadline - runtime.monotonic()
        if remaining <= 0 or not runtime.select([fd], [], [], remaining)[0]:
            _terminate_controlled_process_sync(process_id, runtime)
            raise runtime.error(timeout_message)
        chunk = runtime.read(fd, 65536)
        if not chunk:
            return chunks
        output_size += len(chunk)
        if output_size > runtime.max_output_bytes:
            _terminate_controlled_process_sync(process_id, runtime)
            raise runtime.error(output_message)
        chunks.append(chunk)


def _wait_controlled_process(
    process_id: int, deadline: float, timeout_message: str, runtime: _ControlledSyncRuntime
) -> int:
    while True:
        completed_pid, wait_status = runtime.waitpid(process_id, runtime.wnohang)
        if completed_pid == process_id:
            return wait_status
        remaining = deadline - runtime.monotonic()
        if remaining <= 0:
            _terminate_controlled_process_sync(process_id, runtime)
            raise runtime.error(timeout_message)
        runtime.select([], [], [], min(remaining, 0.05))


def _controlled_file_actions(
    read_fd: int, write_fd: int, input_read_fd: int, input_write_fd: int, runtime: _ControlledSyncRuntime
) -> tuple[tuple[int, int, int] | tuple[int, int], ...]:
    actions: list[tuple[int, int, int] | tuple[int, int]] = [
        (runtime.posix_spawn_dup2, write_fd, 1),
        (runtime.posix_spawn_dup2, write_fd, 2),
        (runtime.posix_spawn_close, read_fd),
        (runtime.posix_spawn_close, write_fd),
    ]
    if input_read_fd >= 0:
        actions.extend(
            (
                (runtime.posix_spawn_dup2, input_read_fd, 0),
                (runtime.posix_spawn_close, input_read_fd),
                (runtime.posix_spawn_close, input_write_fd),
            )
        )
    return tuple(actions)


def _close_controlled_descriptor_or_raise(fd: int, runtime: _ControlledSyncRuntime) -> None:
    error = _close_controlled_descriptor(fd, runtime)
    if error is not None:
        raise error


def _decode_controlled_output(
    chunks: list[bytes],
    wait_status: int | None,
    failure_message: str,
    output_message: str,
    runtime: _ControlledSyncRuntime,
) -> str:
    if wait_status is None or runtime.waitstatus_to_exitcode(wait_status) != 0:
        raise runtime.error(failure_message)
    invalid_output = False
    try:
        output = b"".join(chunks).decode().strip()
    except UnicodeDecodeError:
        invalid_output = True
        output = ""
    if invalid_output:
        raise runtime.error(output_message)
    return output


def _open_controlled_pipes(input_data: bytes | None, runtime: _ControlledSyncRuntime) -> tuple[int, int, int, int]:
    read_fd, write_fd = runtime.pipe()
    try:
        input_read_fd, input_write_fd = runtime.pipe() if input_data is not None else (-1, -1)
    except SETTLEMENT_ERRORS as error:
        cleanup_errors = _close_controlled_descriptors((read_fd, write_fd), runtime)
        if cleanup_errors:
            raise error from cleanup_errors[0]
        raise
    return read_fd, write_fd, input_read_fd, input_write_fd


def _controlled_process_error(
    error: Exception, failure_message: str, runtime: _ControlledSyncRuntime
) -> CatalogValidationError:
    try:
        raise runtime.error(failure_message) from error
    except CatalogValidationError as sanitized:
        return sanitized


def _run_controlled_command(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
    runtime: _ControlledSyncRuntime,
) -> str:
    command, environment = runtime.command_spec(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
    )
    read_fd = write_fd = input_read_fd = input_write_fd = -1
    process_id: int | None = None
    wait_status: int | None = None
    chunks: list[bytes] = []
    primary_error: Exception | None = None
    cleanup_errors: list[Exception] = []
    try:
        read_fd, write_fd, input_read_fd, input_write_fd = _open_controlled_pipes(input_data, runtime)
        deadline = runtime.monotonic() + runtime.timeout_seconds
        file_actions = _controlled_file_actions(read_fd, write_fd, input_read_fd, input_write_fd, runtime)
        process_id = runtime.posix_spawnp(command[0], command, environment, file_actions=file_actions)
        _close_controlled_descriptor_or_raise(write_fd, runtime)
        write_fd = -1
        if input_read_fd >= 0:
            _close_controlled_descriptor_or_raise(input_read_fd, runtime)
            input_read_fd = -1
        if input_write_fd >= 0:
            _write_controlled_input(process_id, input_write_fd, input_data or b"", deadline, timeout_message, runtime)
            _close_controlled_descriptor_or_raise(input_write_fd, runtime)
            input_write_fd = -1
        chunks = _read_controlled_output(process_id, read_fd, deadline, timeout_message, output_message, runtime)
        wait_status = _wait_controlled_process(process_id, deadline, timeout_message, runtime)
    except (AttributeError, OSError) as error:
        if process_id is not None:
            _terminate_controlled_process_sync(process_id, runtime)
        primary_error = _controlled_process_error(error, failure_message, runtime)
    except (KeyboardInterrupt, SystemExit):
        if process_id is not None:
            _terminate_controlled_process_sync(process_id, runtime)
        raise
    except Exception as error:
        primary_error = error
    finally:
        cleanup_errors = _close_controlled_descriptors((read_fd, write_fd, input_read_fd, input_write_fd), runtime)
    if primary_error is not None:
        if cleanup_errors:
            raise primary_error from cleanup_errors[0]
        raise primary_error
    if cleanup_errors:
        raise runtime.error(failure_message) from cleanup_errors[0]
    return _decode_controlled_output(chunks, wait_status, failure_message, output_message, runtime)


def _controlled_command(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
) -> str:
    return _run_controlled_command(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=timeout_message,
        output_message=output_message,
        failure_message=failure_message,
        runtime=_current_controlled_sync_runtime(),
    )


def _make_bound_controlled_command(runtime: _ControlledSyncRuntime) -> Callable[..., str]:
    run_command = _run_controlled_command

    def command(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
        timeout_message: str,
        output_message: str,
        failure_message: str,
    ) -> str:
        return run_command(
            args,
            input_data=input_data,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=timeout_message,
            output_message=output_message,
            failure_message=failure_message,
            runtime=runtime,
        )

    return command


def _compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
    return _controlled_command(
        args,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
        output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
        failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
    )


async def _terminate_controlled_process(
    process: asyncio.subprocess.Process | None,
    *,
    wait_for: Callable[..., Awaitable[object]] = asyncio.wait_for,
    clock: Callable[[], float] = monotonic,
    reap_timeout: float = _CONTROLLED_REAP_TIMEOUT_SECONDS,
) -> None:
    if process is None or process.returncode is not None:
        return
    with contextlib.suppress(OSError):
        process.kill()
    reap_deadline = clock() + reap_timeout
    remaining = reap_deadline - clock()
    if remaining <= 0:
        return
    try:
        await wait_for(process.wait(), timeout=remaining)
    except OSError:
        return


@dataclass(frozen=True, slots=True)
class _ControlledAsyncRuntime:
    command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]]
    error: Callable[[str], CatalogValidationError]
    create_subprocess_exec: Callable[..., Awaitable[asyncio.subprocess.Process]]
    wait_for: Callable[..., Awaitable[object]]
    terminate: Callable[[asyncio.subprocess.Process | None], Awaitable[None]]
    pipe: int
    devnull: int
    stdout: int
    stderr: int
    monotonic: Callable[[], float]
    timeout_seconds: float
    max_output_bytes: int


def _current_controlled_async_runtime(
    *, command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]] | None = None
) -> _ControlledAsyncRuntime:
    return _ControlledAsyncRuntime(
        command_spec=_controlled_command_spec if command_spec is None else command_spec,
        error=_controlled_error,
        create_subprocess_exec=asyncio.create_subprocess_exec,
        wait_for=asyncio.wait_for,
        terminate=_terminate_controlled_process,
        pipe=asyncio.subprocess.PIPE,
        devnull=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        monotonic=monotonic,
        timeout_seconds=_CONTROLLED_COMMAND_TIMEOUT_SECONDS,
        max_output_bytes=_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES,
    )


async def _write_controlled_input_async(
    process: asyncio.subprocess.Process,
    input_data: bytes,
    deadline: float,
    timeout_message: str,
    runtime: _ControlledAsyncRuntime,
) -> None:
    if process.stdin is None:
        raise OSError("controlled command stdin was unavailable")
    remaining = deadline - runtime.monotonic()
    if remaining <= 0:
        await runtime.terminate(process)
        raise runtime.error(timeout_message)
    process.stdin.write(input_data)
    try:
        await runtime.wait_for(process.stdin.drain(), timeout=remaining)
    except TimeoutError:
        await runtime.terminate(process)
        raise runtime.error(timeout_message) from None
    process.stdin.close()


async def _read_controlled_output_async(
    process: asyncio.subprocess.Process,
    deadline: float,
    timeout_message: str,
    output_message: str,
    runtime: _ControlledAsyncRuntime,
) -> list[bytes]:
    if process.stdout is None:
        raise OSError("controlled command stdout was unavailable")
    chunks: list[bytes] = []
    output_size = 0
    while True:
        remaining = deadline - runtime.monotonic()
        if remaining <= 0:
            await runtime.terminate(process)
            raise runtime.error(timeout_message)
        try:
            chunk = cast("bytes", await runtime.wait_for(process.stdout.read(65536), timeout=remaining))
        except TimeoutError:
            await runtime.terminate(process)
            raise runtime.error(timeout_message) from None
        if not chunk:
            return chunks
        output_size += len(chunk)
        if output_size > runtime.max_output_bytes:
            await runtime.terminate(process)
            raise runtime.error(output_message)
        chunks.append(chunk)


async def _wait_controlled_process_async(
    process: asyncio.subprocess.Process, deadline: float, timeout_message: str, runtime: _ControlledAsyncRuntime
) -> None:
    remaining = deadline - runtime.monotonic()
    if remaining <= 0:
        await runtime.terminate(process)
        raise runtime.error(timeout_message)
    try:
        await runtime.wait_for(process.wait(), timeout=remaining)
    except TimeoutError:
        await runtime.terminate(process)
        raise runtime.error(timeout_message) from None


async def _run_controlled_command_async(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
    runtime: _ControlledAsyncRuntime,
) -> str:
    command, environment = runtime.command_spec(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
    )
    process: asyncio.subprocess.Process | None = None
    deadline = runtime.monotonic() + runtime.timeout_seconds
    try:
        remaining = deadline - runtime.monotonic()
        if remaining <= 0:
            raise runtime.error(timeout_message)
        try:
            process = cast(
                "asyncio.subprocess.Process",
                await runtime.wait_for(
                    runtime.create_subprocess_exec(
                        *command,
                        stdin=runtime.pipe if input_data is not None else runtime.devnull,
                        stdout=runtime.stdout,
                        stderr=runtime.stderr,
                        env=environment,
                    ),
                    timeout=remaining,
                ),
            )
        except TimeoutError:
            raise runtime.error(timeout_message) from None
        if input_data is not None:
            await _write_controlled_input_async(process, input_data, deadline, timeout_message, runtime)
        chunks = await _read_controlled_output_async(process, deadline, timeout_message, output_message, runtime)
        await _wait_controlled_process_async(process, deadline, timeout_message, runtime)
    except OSError as error:
        await runtime.terminate(process)
        raise runtime.error(failure_message) from error
    finally:
        await runtime.terminate(process)
    if process is None or process.returncode != 0:
        raise runtime.error(failure_message)
    try:
        return b"".join(chunks).decode().strip()
    except UnicodeDecodeError:
        raise runtime.error(output_message) from None


async def _controlled_command_async(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
) -> str:
    return await _run_controlled_command_async(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=timeout_message,
        output_message=output_message,
        failure_message=failure_message,
        runtime=_current_controlled_async_runtime(),
    )


def _make_bound_controlled_command_async(runtime: _ControlledAsyncRuntime) -> Callable[..., Awaitable[str]]:
    run_command = _run_controlled_command_async

    async def command(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
        timeout_message: str,
        output_message: str,
        failure_message: str,
    ) -> str:
        return await run_command(
            args,
            input_data=input_data,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=timeout_message,
            output_message=output_message,
            failure_message=failure_message,
            runtime=runtime,
        )

    return command


async def _compose_read_async(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
    return await _controlled_command_async(
        args,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
        output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
        failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
    )


def _controlled_patch_response(
    credential: UDataCredential,
    body: Mapping[str, object],
    *,
    command: Callable[..., str] | None = None,
    source_verifier: Callable[..., _ControlledSourceIdentity] | None = None,
    response_parser: Callable[[str], RuntimeResponse] | None = None,
    patch_program: str | None = None,
    source_identity: _ControlledSourceIdentity | None = None,
) -> RuntimeResponse:
    runner = _controlled_command if command is None else command
    verify_source = _verify_controlled_source_and_nonce if source_verifier is None else source_verifier
    parse_response = _parse_controlled_patch_output if response_parser is None else response_parser
    program = _CONTROLLED_PATCH_PROGRAM if patch_program is None else patch_program
    identity = source_identity if source_identity is not None else verify_source()
    token = credential.api_key.reveal() if isinstance(credential.api_key, SecretValue) else credential.api_key
    input_data = json.dumps({"token": token, "body": dict(body)}, separators=(",", ":"), allow_nan=False).encode()
    output = runner(
        ("exec", "-i", identity.udata_container_id, "python", "-c", program),
        input_data=input_data,
        docker_endpoint=identity.docker_endpoint,
        direct=True,
        timeout_message="The controlled uData site PATCH timed out.",
        output_message=_CONTROLLED_SITE_PATCH_TOO_MUCH_OUTPUT,
        failure_message="The controlled uData site PATCH process failed.",
    )
    return parse_response(output)


def _parse_controlled_patch_output(
    output: str, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> RuntimeResponse:
    try:
        result = json.loads(output)
    except (TypeError, ValueError):
        raise error_factory(_CONTROLLED_SITE_PATCH_INVALID_OUTPUT) from None
    if not isinstance(result, Mapping):
        raise error_factory(_CONTROLLED_SITE_PATCH_INVALID_OUTPUT)
    status = result.get("status")
    content_type = result.get("content_type")
    location = result.get("location")
    response_body = result.get("body")
    if (
        type(status) is not int
        or not 100 <= status <= 599
        or not isinstance(content_type, str)
        or not isinstance(location, str)
        or not isinstance(response_body, str)
    ):
        raise error_factory(_CONTROLLED_SITE_PATCH_INVALID_OUTPUT)
    headers: dict[str, str] = {"Content-Type": content_type}
    if location:
        headers["Location"] = location
    encoded_body = response_body.encode()
    if len(encoded_body) > 8192:
        raise error_factory(_CONTROLLED_SITE_PATCH_TOO_MUCH_OUTPUT)
    return RuntimeResponse(status, headers, encoded_body)


async def _controlled_patch_response_async(
    credential: UDataCredential,
    body: Mapping[str, object],
    *,
    command: Callable[..., Awaitable[str]] | None = None,
    source_verifier: Callable[..., Awaitable[_ControlledSourceIdentity]] | None = None,
    response_parser: Callable[[str], RuntimeResponse] | None = None,
    patch_program: str | None = None,
    source_identity: _ControlledSourceIdentity | None = None,
) -> RuntimeResponse:
    runner = _controlled_command_async if command is None else command
    verify_source = _verify_controlled_source_and_nonce_async if source_verifier is None else source_verifier
    parse_response = _parse_controlled_patch_output if response_parser is None else response_parser
    program = _CONTROLLED_PATCH_PROGRAM if patch_program is None else patch_program
    identity = source_identity if source_identity is not None else await verify_source()
    token = credential.api_key.reveal() if isinstance(credential.api_key, SecretValue) else credential.api_key
    input_data = json.dumps({"token": token, "body": dict(body)}, separators=(",", ":"), allow_nan=False).encode()
    output = await runner(
        ("exec", "-i", identity.udata_container_id, "python", "-c", program),
        input_data=input_data,
        docker_endpoint=identity.docker_endpoint,
        direct=True,
        timeout_message="The controlled uData site PATCH timed out.",
        output_message=_CONTROLLED_SITE_PATCH_TOO_MUCH_OUTPUT,
        failure_message="The controlled uData site PATCH process failed.",
    )
    return parse_response(output)


def _controlled_source_nonce(
    *,
    nonce_getter: Callable[..., str | None] = os.environ.get,
    compose_file: Path = _CONTROLLED_COMPOSE_FILE,
    evidence_root: Path = _CONTROLLED_EVIDENCE_ROOT,
    compose_sha256: str = _CONTROLLED_COMPOSE_SHA256,
    dockerfile_sha256: str = _CONTROLLED_DOCKERFILE_SHA256,
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
) -> str:
    nonce = nonce_getter("UDATA_EVIDENCE_STACK_NONCE")
    if not nonce:
        raise error_factory("The controlled uData stack nonce is unavailable.")
    try:
        compose_text = compose_file.read_text(encoding="utf-8")
        dockerfile_text = evidence_root.joinpath("Dockerfile").read_text(encoding="utf-8")
    except OSError as error:
        raise error_factory("The controlled uData source identity is unavailable.") from error
    if hashlib.sha256(compose_text.encode()).hexdigest() != compose_sha256:
        raise error_factory("The controlled uData compose identity does not match the approved source.")
    if hashlib.sha256(dockerfile_text.encode()).hexdigest() != dockerfile_sha256:
        raise error_factory("The controlled uData image identity does not match the approved source.")
    return nonce


def _validate_controlled_context_endpoint(
    context_endpoint: str, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> str:
    try:
        endpoint = json.loads(context_endpoint)
    except (TypeError, ValueError):
        raise error_factory("The active Docker context identity is invalid.") from None
    parsed_endpoint = urlsplit(endpoint) if isinstance(endpoint, str) else None
    if (
        parsed_endpoint is None
        or parsed_endpoint.scheme != "unix"
        or bool(parsed_endpoint.netloc)
        or not parsed_endpoint.path.startswith("/")
        or bool(parsed_endpoint.query)
        or bool(parsed_endpoint.fragment)
    ):
        raise error_factory(_CONTROLLED_UDATA_LOCAL_DOCKER_CONTEXT)
    return endpoint


def _parse_controlled_json_fields(
    output: str, field_count: int, error_factory: Callable[[str], CatalogValidationError]
) -> tuple[object, ...]:
    fields = output.split(" ", field_count - 1)
    if len(fields) != field_count:
        raise error_factory("The controlled uData image identity output is invalid.")
    try:
        return tuple(json.loads(field) for field in fields)
    except (TypeError, ValueError):
        raise error_factory("The controlled uData image identity output is invalid.") from None


def _controlled_container_id(output: str, error_factory: Callable[[str], CatalogValidationError]) -> str:
    container_ids = [value for value in output.splitlines() if value]
    if len(container_ids) != 1 or re.fullmatch(r"[0-9a-f]{64}", container_ids[0]) is None:
        raise error_factory("The controlled uData service container identity is invalid.")
    return container_ids[0]


def _validate_controlled_container_image(
    fields: tuple[object, ...],
    container_id: str,
    service: str,
    expected_image: tuple[str, str, str] | None,
    udata_image_repository: str,
    udata_image_spec: tuple[str, str, str],
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[str, str, str | None]:
    inspected_container_id, image_id, config_image = fields
    if (
        inspected_container_id != container_id
        or not isinstance(image_id, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
        or not isinstance(config_image, str)
    ):
        raise error_factory("The controlled uData container image identity is invalid.")
    if (
        not isinstance(expected_image, tuple)
        or len(expected_image) != 3
        or not all(isinstance(value, str) for value in expected_image)
    ):
        raise error_factory("The controlled uData image allowlist is invalid.")
    expected_config_image, expected_image_id, expected_repository_digest = expected_image
    if service == "udata":
        if (
            expected_image != udata_image_spec
            or expected_config_image != udata_image_repository
            or expected_repository_digest != udata_image_repository
            or config_image != expected_config_image
        ):
            raise error_factory(_CONTROLLED_SERVICE_IMAGE_NOT_APPROVED)
        return config_image, image_id, None
    if config_image != expected_config_image:
        raise error_factory("A controlled uData dependency image is not approved.")
    if image_id != expected_image_id:
        raise error_factory("The controlled uData dependency image ID is not approved.")
    return config_image, image_id, expected_repository_digest


def _validate_controlled_image_digest(
    fields: tuple[object, ...],
    image_id: str,
    expected_repository_digest: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> None:
    inspected_image_id, repository_digests = fields
    if not isinstance(repository_digests, list) or not all(isinstance(value, str) for value in repository_digests):
        raise error_factory("The controlled uData image digest output is invalid.")
    if inspected_image_id != image_id:
        raise error_factory("The controlled uData image ID does not match its container.")
    if expected_repository_digest not in repository_digests:
        raise error_factory("The controlled uData image repository digest is not approved.")


def _controlled_service_image_identity(
    read: Callable[..., str],
    service: str,
    docker_endpoint: str,
    expected_image: tuple[str, str, str] | None,
    udata_image_repository: str,
    udata_image_spec: tuple[str, str, str],
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[str, str]:
    container_id = _controlled_container_id(read("ps", "-q", service, docker_endpoint=docker_endpoint), error_factory)
    container_fields = _parse_controlled_json_fields(
        read(
            "inspect",
            "--format",
            "{{json .Id}} {{json .Image}} {{json .Config.Image}}",
            container_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        3,
        error_factory,
    )
    config_image, image_id, expected_repository_digest = _validate_controlled_container_image(
        container_fields,
        container_id,
        service,
        expected_image,
        udata_image_repository,
        udata_image_spec,
        error_factory,
    )
    image_fields = _parse_controlled_json_fields(
        read(
            "image",
            "inspect",
            "--format",
            "{{json .Id}} {{json .RepoDigests}}",
            image_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        2,
        error_factory,
    )
    if expected_repository_digest is not None:
        _validate_controlled_image_digest(image_fields, image_id, expected_repository_digest, error_factory)
    return container_id, f"{service}|{config_image}|{image_id}|{expected_repository_digest}"


async def _controlled_service_image_identity_async(
    read: Callable[..., Awaitable[str]],
    service: str,
    docker_endpoint: str,
    expected_image: tuple[str, str, str] | None,
    udata_image_repository: str,
    udata_image_spec: tuple[str, str, str],
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[str, str]:
    container_id = _controlled_container_id(
        await read("ps", "-q", service, docker_endpoint=docker_endpoint), error_factory
    )
    container_fields = _parse_controlled_json_fields(
        await read(
            "inspect",
            "--format",
            "{{json .Id}} {{json .Image}} {{json .Config.Image}}",
            container_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        3,
        error_factory,
    )
    config_image, image_id, expected_repository_digest = _validate_controlled_container_image(
        container_fields,
        container_id,
        service,
        expected_image,
        udata_image_repository,
        udata_image_spec,
        error_factory,
    )
    image_fields = _parse_controlled_json_fields(
        await read(
            "image",
            "inspect",
            "--format",
            "{{json .Id}} {{json .RepoDigests}}",
            image_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        2,
        error_factory,
    )
    if expected_repository_digest is not None:
        _validate_controlled_image_digest(image_fields, image_id, expected_repository_digest, error_factory)
    return container_id, f"{service}|{config_image}|{image_id}|{expected_repository_digest}"


def _controlled_source_commit_args(container_id: str) -> tuple[str, ...]:
    """Return the compose exec arguments that read the controlled uData source commit."""
    return ("exec", container_id, "git", "-C", "/opt/udata", "rev-parse", "HEAD")


def _controlled_nonce_args(container_id: str) -> tuple[str, ...]:
    """Return the compose exec arguments that read the controlled uData stack nonce."""
    return ("exec", container_id, "printenv", "UDATA_EVIDENCE_STACK_NONCE")


def _controlled_version_args(container_id: str) -> tuple[str, ...]:
    """Return the compose exec arguments that read the running uData service version."""
    return (
        "exec",
        container_id,
        "python",
        "-c",
        "from udata.utils import get_udata_version; print(get_udata_version())",
    )


def _require_controlled_source_commit(
    observed: str,
    source_commit: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> None:
    """Reject a controlled uData service whose checked-out source drifted from the approved commit."""
    if observed != source_commit:
        raise error_factory("The controlled uData service source does not match the approved commit.")


def _require_controlled_nonce(
    observed: str,
    nonce: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> None:
    """Reject a controlled uData service whose stack nonce drifted from the local evidence nonce."""
    if observed != nonce:
        raise error_factory("The controlled uData service nonce does not match the local evidence nonce.")


def _require_controlled_version(
    observed: str,
    version: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> None:
    """Reject a controlled uData service whose running version is not the pinned build contract."""
    if observed != version:
        raise error_factory(_CONTROLLED_SERVICE_IMAGE_NOT_APPROVED)


def _controlled_image_identities(
    read: Callable[..., str],
    service_names: tuple[str, ...],
    dependency_images: Mapping[str, tuple[str, str, str]],
    *,
    docker_endpoint: str,
    udata_image_repository: str,
    udata_image_spec: tuple[str, str, str],
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[list[str], str]:
    """Return the image identity of every controlled service and the uData container id."""
    image_identities: list[str] = []
    udata_container_id = ""
    for service in service_names:
        container_id, image_identity = _controlled_service_image_identity(
            read,
            service,
            docker_endpoint,
            udata_image_spec if service == "udata" else dependency_images.get(service),
            udata_image_repository,
            udata_image_spec,
            error_factory,
        )
        image_identities.append(image_identity)
        if service == "udata":
            udata_container_id = container_id
    return image_identities, udata_container_id


def _verify_controlled_source_and_nonce(
    *,
    compose_read: Callable[..., str] | None = None,
    source_nonce: Callable[[], str] | None = None,
    context_validator: Callable[[str], str] | None = None,
    source_commit: str = _CONTROLLED_SOURCE_COMMIT,
    service_names: tuple[str, ...] = _CONTROLLED_SERVICE_NAMES,
    expected_services: frozenset[str] = frozenset(_CONTROLLED_SERVICE_NAMES),
    dependency_image_specs: tuple[tuple[str, str, str, str], ...] = _CONTROLLED_DEPENDENCY_IMAGE_SPECS,
    udata_image_repository: str = _CONTROLLED_UDATA_IMAGE_REPOSITORY,
    udata_image_spec: tuple[str, str, str] = _CONTROLLED_UDATA_IMAGE_SPEC,
    udata_version: str = _CONTROLLED_UDATA_VERSION,
    expected_port: str = "127.0.0.1:5640",
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
    docker_endpoint: str | None = None,
) -> _ControlledSourceIdentity:
    read = _compose_read if compose_read is None else compose_read
    nonce_reader = _controlled_source_nonce if source_nonce is None else source_nonce
    validate_context = _validate_controlled_context_endpoint if context_validator is None else context_validator
    nonce = nonce_reader()
    if docker_endpoint is None:
        context_endpoint = read("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
        docker_endpoint = validate_context(context_endpoint)
    else:
        validate_context(json.dumps(docker_endpoint))
    services = read("ps", "--status", "running", "--services", docker_endpoint=docker_endpoint).splitlines()
    if len(services) != len(expected_services) or set(services) != expected_services:
        raise error_factory("The running services do not match the controlled uData stack.")
    if read("port", "udata", "7000", docker_endpoint=docker_endpoint) != expected_port:
        raise error_factory("The controlled uData service is not bound to the approved loopback port.")
    dependency_images = {
        service: (config_image, image_id, repository_digest)
        for service, config_image, image_id, repository_digest in dependency_image_specs
    }
    image_identities, udata_container_id = _controlled_image_identities(
        read,
        service_names,
        dependency_images,
        docker_endpoint=docker_endpoint,
        udata_image_repository=udata_image_repository,
        udata_image_spec=udata_image_spec,
        error_factory=error_factory,
    )
    _require_controlled_source_commit(
        read(
            *_controlled_source_commit_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        source_commit,
        error_factory,
    )
    _require_controlled_nonce(
        read(
            *_controlled_nonce_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        nonce,
        error_factory,
    )
    _require_controlled_version(
        read(
            *_controlled_version_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        udata_version,
        error_factory,
    )
    return _ControlledSourceIdentity(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
        docker_endpoint=docker_endpoint,
        udata_container_id=udata_container_id,
        image_identities=tuple(image_identities),
    )


async def _verify_controlled_source_and_nonce_async(
    *,
    compose_read: Callable[..., Awaitable[str]] | None = None,
    source_nonce: Callable[[], str] | None = None,
    context_validator: Callable[[str], str] | None = None,
    source_commit: str = _CONTROLLED_SOURCE_COMMIT,
    service_names: tuple[str, ...] = _CONTROLLED_SERVICE_NAMES,
    expected_services: frozenset[str] = frozenset(_CONTROLLED_SERVICE_NAMES),
    dependency_image_specs: tuple[tuple[str, str, str, str], ...] = _CONTROLLED_DEPENDENCY_IMAGE_SPECS,
    udata_image_repository: str = _CONTROLLED_UDATA_IMAGE_REPOSITORY,
    udata_image_spec: tuple[str, str, str] = _CONTROLLED_UDATA_IMAGE_SPEC,
    udata_version: str = _CONTROLLED_UDATA_VERSION,
    expected_port: str = "127.0.0.1:5640",
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
    docker_endpoint: str | None = None,
) -> _ControlledSourceIdentity:
    read = _compose_read_async if compose_read is None else compose_read
    nonce_reader = _controlled_source_nonce if source_nonce is None else source_nonce
    validate_context = _validate_controlled_context_endpoint if context_validator is None else context_validator
    nonce = nonce_reader()
    if docker_endpoint is None:
        context_endpoint = await read("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
        docker_endpoint = validate_context(context_endpoint)
    else:
        validate_context(json.dumps(docker_endpoint))
    services = (await read("ps", "--status", "running", "--services", docker_endpoint=docker_endpoint)).splitlines()
    if len(services) != len(expected_services) or set(services) != expected_services:
        raise error_factory("The running services do not match the controlled uData stack.")
    if await read("port", "udata", "7000", docker_endpoint=docker_endpoint) != expected_port:
        raise error_factory("The controlled uData service is not bound to the approved loopback port.")
    dependency_images = {
        service: (config_image, image_id, repository_digest)
        for service, config_image, image_id, repository_digest in dependency_image_specs
    }
    image_identities: list[str] = []
    udata_container_id = ""
    for service in service_names:
        container_id, image_identity = await _controlled_service_image_identity_async(
            read,
            service,
            docker_endpoint,
            udata_image_spec if service == "udata" else dependency_images.get(service),
            udata_image_repository,
            udata_image_spec,
            error_factory,
        )
        image_identities.append(image_identity)
        if service == "udata":
            udata_container_id = container_id
    _require_controlled_source_commit(
        await read(
            *_controlled_source_commit_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        source_commit,
        error_factory,
    )
    _require_controlled_nonce(
        await read(
            *_controlled_nonce_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        nonce,
        error_factory,
    )
    _require_controlled_version(
        await read(
            *_controlled_version_args(udata_container_id),
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        udata_version,
        error_factory,
    )
    return _ControlledSourceIdentity(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
        docker_endpoint=docker_endpoint,
        udata_container_id=udata_container_id,
        image_identities=tuple(image_identities),
    )


def _controlled_peer_evidence(
    response: RuntimeResponse, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> str:
    content_type = _response_header(response, "content-type")
    if (
        not 200 <= response.status_code < 300
        or content_type is None
        or content_type.split(";", 1)[0] != _JSON_MEDIA_TYPE
    ):
        raise error_factory("The controlled uData peer did not return its expected site identity.")
    invalid_payload = False
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError):
        invalid_payload = True
        payload = None
    if invalid_payload:
        raise error_factory("The controlled uData peer returned an invalid site identity.")
    if (
        not isinstance(payload, Mapping)
        or not isinstance(payload.get("id"), str)
        or not payload["id"]
        or payload.get("version") != declared_udata_profile().profile_version
    ):
        raise error_factory("The controlled uData peer did not match the seeded site identity.")
    return payload["id"]


def _verify_controlled_sync_stack(
    transport: CatalogTransport,
    *,
    source_verifier: Callable[..., _ControlledSourceIdentity] | None = None,
    peer_evidence: Callable[[RuntimeResponse], str] | None = None,
    site_probe: Callable[[str, str], RuntimeResponse] | None = None,
    controlled_origin: str = _CONTROLLED_ORIGIN,
    docker_endpoint: str | None = None,
) -> _ControlledVerification:
    verify_source = _verify_controlled_source_and_nonce if source_verifier is None else source_verifier
    decode_peer = _controlled_peer_evidence if peer_evidence is None else peer_evidence
    identity = verify_source(docker_endpoint=docker_endpoint)
    response = (
        transport.send(
            RuntimeRequest(
                method="GET",
                url=f"{controlled_origin}/api/1/site/",
                headers={},
                redirect_policy=RedirectPolicy.NO_FOLLOW,
                max_response_bytes=8192,
            )
        )
        if site_probe is None
        else site_probe(identity.docker_endpoint, identity.udata_container_id)
    )
    evidence = _ControlledStackEvidence(
        nonce_sha256=identity.nonce_sha256,
        site_id=decode_peer(response),
        docker_endpoint_sha256=hashlib.sha256(identity.docker_endpoint.encode()).hexdigest(),
        image_digests=identity.image_identities,
    )
    return _ControlledVerification(identity=identity, evidence=evidence)


async def _verify_controlled_async_stack(
    transport: AsyncCatalogTransport,
    *,
    source_verifier: Callable[..., Awaitable[_ControlledSourceIdentity]] | None = None,
    peer_evidence: Callable[[RuntimeResponse], str] | None = None,
    site_probe: Callable[[str, str], Awaitable[RuntimeResponse]] | None = None,
    controlled_origin: str = _CONTROLLED_ORIGIN,
    docker_endpoint: str | None = None,
) -> _ControlledVerification:
    verify_source = _verify_controlled_source_and_nonce_async if source_verifier is None else source_verifier
    decode_peer = _controlled_peer_evidence if peer_evidence is None else peer_evidence
    identity = await verify_source(docker_endpoint=docker_endpoint)
    response = (
        await transport.send(
            RuntimeRequest(
                method="GET",
                url=f"{controlled_origin}/api/1/site/",
                headers={},
                redirect_policy=RedirectPolicy.NO_FOLLOW,
                max_response_bytes=8192,
            )
        )
        if site_probe is None
        else await site_probe(identity.docker_endpoint, identity.udata_container_id)
    )
    evidence = _ControlledStackEvidence(
        nonce_sha256=identity.nonce_sha256,
        site_id=decode_peer(response),
        docker_endpoint_sha256=hashlib.sha256(identity.docker_endpoint.encode()).hexdigest(),
        image_digests=identity.image_identities,
    )
    return _ControlledVerification(identity=identity, evidence=evidence)


def _make_controlled_compose_read(command: Callable[..., str]) -> Callable[..., str]:
    def compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
        return command(
            args,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
            output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
            failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
        )

    return compose_read


def _make_controlled_compose_read_async(command: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
    async def compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
        return await command(
            args,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
            output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
            failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
        )

    return compose_read


def _make_controlled_site_probe(
    command: Callable[..., str], response_parser: Callable[[str], RuntimeResponse], program: str
) -> Callable[[str, str], RuntimeResponse]:
    def probe(docker_endpoint: str, container_id: str) -> RuntimeResponse:
        output = command(
            ("exec", "-i", container_id, "python", "-c", program),
            docker_endpoint=docker_endpoint,
            direct=True,
            timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
            output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
            failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
        )
        return response_parser(output)

    return probe


def _make_controlled_site_probe_async(
    command: Callable[..., Awaitable[str]], response_parser: Callable[[str], RuntimeResponse], program: str
) -> Callable[[str, str], Awaitable[RuntimeResponse]]:
    async def probe(docker_endpoint: str, container_id: str) -> RuntimeResponse:
        output = await command(
            ("exec", "-i", container_id, "python", "-c", program),
            docker_endpoint=docker_endpoint,
            direct=True,
            timeout_message=_CONTROLLED_STACK_IDENTITY_TIMEOUT,
            output_message=_CONTROLLED_STACK_IDENTITY_TOO_MUCH_OUTPUT,
            failure_message=_CONTROLLED_STACK_IDENTITY_FAILED,
        )
        return response_parser(output)

    return probe


def _make_controlled_sync_operations(command: Callable[..., str]) -> _ControlledSyncOperations:
    compose_read = _make_controlled_compose_read(command)
    source_checker = _verify_controlled_source_and_nonce
    source_nonce = _controlled_source_nonce
    context_validator = _validate_controlled_context_endpoint
    stack_verifier = _verify_controlled_sync_stack
    peer_evidence = _controlled_peer_evidence
    patch_response = _controlled_patch_response
    response_parser = _parse_controlled_patch_output
    site_probe = _make_controlled_site_probe(command, response_parser, _CONTROLLED_SITE_PROBE_PROGRAM)
    patch_program = _CONTROLLED_PATCH_PROGRAM

    def verify_source(*, docker_endpoint: str | None = None) -> _ControlledSourceIdentity:
        return source_checker(
            compose_read=compose_read,
            source_nonce=source_nonce,
            context_validator=context_validator,
            docker_endpoint=docker_endpoint,
        )

    def verify(transport: CatalogTransport, *, docker_endpoint: str | None = None) -> _ControlledVerification:
        return stack_verifier(
            transport,
            source_verifier=verify_source,
            peer_evidence=peer_evidence,
            site_probe=site_probe,
            docker_endpoint=docker_endpoint,
        )

    def dispatch(
        transport: CatalogTransport,
        credential: UDataCredential,
        body: Mapping[str, object],
        *,
        docker_endpoint: str,
    ) -> RuntimeResponse:
        verification = verify(transport, docker_endpoint=docker_endpoint)
        return patch_response(
            credential,
            body,
            command=command,
            response_parser=response_parser,
            patch_program=patch_program,
            source_identity=verification.identity,
        )

    return _ControlledSyncOperations(verify=verify, dispatch=dispatch)


def _make_controlled_async_operations(command: Callable[..., Awaitable[str]]) -> _ControlledAsyncOperations:
    compose_read = _make_controlled_compose_read_async(command)
    source_checker = _verify_controlled_source_and_nonce_async
    source_nonce = _controlled_source_nonce
    context_validator = _validate_controlled_context_endpoint
    stack_verifier = _verify_controlled_async_stack
    peer_evidence = _controlled_peer_evidence
    patch_response = _controlled_patch_response_async
    response_parser = _parse_controlled_patch_output
    site_probe = _make_controlled_site_probe_async(command, response_parser, _CONTROLLED_SITE_PROBE_PROGRAM)
    patch_program = _CONTROLLED_PATCH_PROGRAM

    async def verify_source(*, docker_endpoint: str | None = None) -> _ControlledSourceIdentity:
        return await source_checker(
            compose_read=compose_read,
            source_nonce=source_nonce,
            context_validator=context_validator,
            docker_endpoint=docker_endpoint,
        )

    async def verify(
        transport: AsyncCatalogTransport, *, docker_endpoint: str | None = None
    ) -> _ControlledVerification:
        return await stack_verifier(
            transport,
            source_verifier=verify_source,
            peer_evidence=peer_evidence,
            site_probe=site_probe,
            docker_endpoint=docker_endpoint,
        )

    async def dispatch(
        transport: AsyncCatalogTransport,
        credential: UDataCredential,
        body: Mapping[str, object],
        *,
        docker_endpoint: str,
    ) -> RuntimeResponse:
        verification = await verify(transport, docker_endpoint=docker_endpoint)
        return await patch_response(
            credential,
            body,
            command=command,
            response_parser=response_parser,
            patch_program=patch_program,
            source_identity=verification.identity,
        )

    return _ControlledAsyncOperations(verify=verify, dispatch=dispatch)


def _require_controlled_settings(settings: UDataClientSettings, controlled_origin: str = _CONTROLLED_ORIGIN) -> None:
    if settings.base_url != controlled_origin:
        raise _controlled_error("Controlled uData mutations require the approved loopback origin.")
    if settings.sync_transport is not None or settings.async_transport is not None:
        raise _controlled_error("Controlled uData mutations cannot use caller-provided transports.")


def _create_controlled_sync_client(settings: UDataClientSettings) -> SyncUDataClient:
    """Construct a stock synchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledSyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.retry_sleep if settings.retry_sleep is not None else sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        root_export_max_bytes=settings.root_export_max_bytes,
        owns_transport=True,
        probe_runner=settings.probe_runner,
    )
    _controlled_dispatch_gate.bind_sync_client(client, transport)
    return client


async def _create_controlled_async_client(settings: UDataClientSettings) -> AsyncUDataClient:
    """Construct a stock asynchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledAsyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
    await transport.verify()
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.async_retry_sleep if settings.async_retry_sleep is not None else asyncio.sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        root_export_max_bytes=settings.root_export_max_bytes,
        owns_transport=True,
        async_probe_runner=settings.async_probe_runner,
    )
    _controlled_dispatch_gate.bind_async_client(client, transport)
    return client


def _build_controlled_transports():
    clock = monotonic
    trusted_command_spec = _make_bound_controlled_command_spec()
    trusted_sync_operations = _make_controlled_sync_operations(
        _make_bound_controlled_command(_current_controlled_sync_runtime(command_spec=trusted_command_spec))
    )
    trusted_async_operations = _make_controlled_async_operations(
        _make_bound_controlled_command_async(_current_controlled_async_runtime(command_spec=trusted_command_spec))
    )
    sync_transport = _build_controlled_sync_transport(_controlled_sync_registry, trusted_sync_operations, clock)
    async_transport = _build_controlled_async_transport(_controlled_async_registry, trusted_async_operations, clock)
    return sync_transport, async_transport


_ControlledSyncTransport, _ControlledAsyncTransport = _build_controlled_transports()
