"""Minimal asynchronous JSON-RPC client for Codex app-server."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any


class CodexAppServerError(RuntimeError):
    """Raised when the Codex app-server process or an RPC request fails."""


NotificationHandler = Callable[[str, dict[str, Any]], Awaitable[None]]
ServerRequestHandler = Callable[[dict[str, Any]], Awaitable[None]]
ClosedHandler = Callable[[Exception], Awaitable[None]]


class AppServerClient:
    """Manage one Codex app-server process and its JSON-RPC stream."""

    def __init__(
        self,
        command: str,
        *,
        on_notification: NotificationHandler,
        on_server_request: ServerRequestHandler,
        on_closed: ClosedHandler,
        logger: Any,
    ) -> None:
        self.command = command
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.on_closed = on_closed
        self.logger = logger
        self.process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._start_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending: dict[int | str, asyncio.Future] = {}
        self._server_request_tasks: set[asyncio.Task] = set()
        self._next_id = 1
        self._closing = False
        self._initialized = False

    async def start(self) -> None:
        """Start the subprocess and complete the app-server handshake."""
        async with self._start_lock:
            if self._initialized:
                return
            if self.process is None:
                argv = self._build_argv()
                try:
                    self.process = await asyncio.create_subprocess_exec(
                        *argv,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                except (OSError, ValueError) as exc:
                    raise CodexAppServerError(f"无法启动 Codex CLI：{exc}") from exc
                self._closing = False
                self._reader_task = asyncio.create_task(self._read_stdout())
                self._stderr_task = asyncio.create_task(self._read_stderr())

            await self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "astrbot-codex",
                        "title": "AstrBot Codex Plugin",
                        "version": "0.1.0",
                    },
                    "capabilities": {
                        "experimentalApi": True,
                        "requestAttestation": False,
                    },
                },
            )
            await self.notify("initialized")
            self._initialized = True

    def _build_argv(self) -> list[str]:
        """Resolve common npm shims to Node so Windows does not need a shell."""
        configured = self.command.strip().strip('"')
        if not configured:
            raise CodexAppServerError("Codex CLI 命令不能为空。")
        executable = shutil.which(configured) or configured
        executable_path = Path(executable)

        if os.name == "nt" and executable_path.suffix.lower() in {".cmd", ".ps1"}:
            script = (
                executable_path.parent
                / "node_modules"
                / "@openai"
                / "codex"
                / "bin"
                / "codex.js"
            )
            node = executable_path.parent / "node.exe"
            node_command = str(node) if node.is_file() else shutil.which("node")
            if script.is_file() and node_command:
                return [node_command, str(script), "app-server", "--listen", "stdio://"]
            if executable_path.suffix.lower() == ".ps1":
                powershell = shutil.which("pwsh") or shutil.which("powershell")
                if powershell:
                    return [
                        powershell,
                        "-NoLogo",
                        "-NoProfile",
                        "-File",
                        str(executable_path),
                        "app-server",
                        "--listen",
                        "stdio://",
                    ]
            command_line = subprocess.list2cmdline(
                [str(executable_path), "app-server", "--listen", "stdio://"]
            )
            return [
                os.environ.get("COMSPEC", "cmd.exe"),
                "/d",
                "/s",
                "/c",
                command_line,
            ]

        return [executable, "app-server", "--listen", "stdio://"]

    async def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send a JSON-RPC request and return its result.

        Args:
            method: App-server method name.
            params: Request parameters.

        Returns:
            The JSON result from app-server.

        Raises:
            CodexAppServerError: If the process is unavailable or rejects the call.
        """
        if self.process is None or self.process.stdin is None:
            raise CodexAppServerError("Codex app-server 尚未启动。")
        request_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        message: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            message["params"] = params
        try:
            await self._send(message)
            return await asyncio.wait_for(future, timeout=120)
        except asyncio.TimeoutError as exc:
            raise CodexAppServerError(f"Codex RPC {method} 等待超时。") from exc
        finally:
            self._pending.pop(request_id, None)

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Send a JSON-RPC notification without waiting for a response."""
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        await self._send(message)

    async def respond_to_server_request(
        self,
        request_id: int | str,
        method: str,
        response: dict[str, Any],
    ) -> None:
        """Send a typed response to a Codex approval request."""
        await self._send(
            {"jsonrpc": "2.0", "method": method, "id": request_id, "response": response}
        )

    async def reject_server_request(self, request_id: int | str, message: str) -> None:
        """Reject a server request that has no supported client-side handler."""
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": message},
            }
        )

    async def _send(self, message: dict[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise CodexAppServerError("Codex app-server stdin 不可用。")
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._write_lock:
            self.process.stdin.write((payload + "\n").encode("utf-8"))
            try:
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise CodexAppServerError("Codex app-server 已关闭。") from exc

    async def _read_stdout(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            return
        reason: Exception = CodexAppServerError("Codex app-server 输出流已关闭。")
        try:
            async for raw_line in process.stdout:
                try:
                    message = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self.logger.warning("忽略无法解析的 Codex app-server 输出行")
                    continue
                await self._dispatch(message)
            return_code = await process.wait()
            reason = CodexAppServerError(
                f"Codex app-server 已退出（退出码 {return_code}）。"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = CodexAppServerError(f"Codex app-server 读取失败：{exc}")
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(reason)
            self._initialized = False
            if not self._closing:
                await self.on_closed(reason)

    async def _dispatch(self, message: dict[str, Any]) -> None:
        if "id" in message and "method" not in message:
            future = self._pending.get(message["id"])
            if future is None or future.done():
                return
            if "error" in message:
                error = message["error"]
                detail = (
                    error.get("message", "未知错误")
                    if isinstance(error, dict)
                    else error
                )
                future.set_exception(CodexAppServerError(str(detail)))
            else:
                future.set_result(message.get("result"))
            return

        method = message.get("method")
        if not isinstance(method, str):
            return
        if "id" in message:
            task = asyncio.create_task(self._run_server_request(message))
            self._server_request_tasks.add(task)
            task.add_done_callback(self._server_request_tasks.discard)
            return
        params = message.get("params")
        if isinstance(params, dict):
            try:
                await self.on_notification(method, params)
            except Exception:
                self.logger.exception("Codex app-server notification handler failed")

    async def _run_server_request(self, message: dict[str, Any]) -> None:
        try:
            await self.on_server_request(message)
        except Exception:
            self.logger.exception("Codex app-server request handler failed")

    async def _read_stderr(self) -> None:
        process = self.process
        if process is None or process.stderr is None:
            return
        try:
            async for raw_line in process.stderr:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if line:
                    self.logger.debug("Codex app-server: %s", line)
        except asyncio.CancelledError:
            raise

    async def close(self) -> None:
        """Stop the app-server process and its stream readers."""
        self._closing = True
        for future in self._pending.values():
            if not future.done():
                future.set_exception(CodexAppServerError("Codex app-server 已关闭。"))
        for task in self._server_request_tasks:
            task.cancel()
        if self._server_request_tasks:
            await asyncio.gather(*self._server_request_tasks, return_exceptions=True)
        self._server_request_tasks.clear()
        process = self.process
        if process is not None and process.returncode is None:
            if process.stdin:
                process.stdin.close()
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=5)
            except ProcessLookupError:
                await process.wait()
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task),
            return_exceptions=True,
        )
        self.process = None
        self._reader_task = None
        self._stderr_task = None
        self._initialized = False
