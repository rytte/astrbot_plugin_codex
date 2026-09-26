"""Control Codex app-server threads from private AstrBot conversations."""

from __future__ import annotations

import asyncio
import html
import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PureWindowsPath
from time import monotonic
from typing import Any, Iterable
from uuid import uuid4

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import File, Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.platform.message_type import MessageType
from astrbot.core.star.filter.command import GreedyStr
from astrbot.core.workspace import resolve_workspace_root_for_umo

from .app_server import AppServerClient, CodexAppServerError


_REPORT_EVENT_LIMIT = 500
_REPORT_EVENT_TEXT_LIMIT = 32_000
_REPORT_FIELD_TEXT_LIMIT = 16_000
_REPORT_CONTAINER_LIMIT = 100
_REPORT_NODE_LIMIT = 1_000
_REPORT_RESULT_PREVIEW_LIMIT = 800
_REPORT_TRUNCATION = "\n…（报告内容已截断）…\n"


@dataclass
class CodexBinding:
    """Persisted association between an AstrBot conversation and a Codex thread."""

    umo: str
    thread_id: str
    cwd: str
    model: str | None = None
    reasoning_effort: str | None = None
    enabled: bool = True
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class ActiveTurn:
    """In-flight Codex operation and its accumulated assistant response."""

    binding: CodexBinding
    thread_id: str
    event: AstrMessageEvent
    completed: asyncio.Future
    request_text: str = ""
    message_deltas: dict[str, list[str]] = field(default_factory=dict)
    unassigned_deltas: list[str] = field(default_factory=list)
    sent_message_ids: set[str] = field(default_factory=set)
    sent_message_texts: set[str] = field(default_factory=set)
    reply_sent: bool = False
    delivery_task: asyncio.Task | None = None
    turn_id: str | None = None
    turn_started: asyncio.Event = field(default_factory=asyncio.Event)
    suppress_reply: bool = False
    interrupt_requested: bool = False
    success_message: str | None = None
    task: asyncio.Task | None = None
    activity_events: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=_REPORT_EVENT_LIMIT)
    )
    activity_truncated: bool = False
    had_reportable_activity: bool = False
    active_items: dict[str, str] = field(default_factory=dict)
    current_activity: str = "等待 Codex 开始"
    started_at: float = field(default_factory=monotonic)


@dataclass
class PendingApproval:
    """Approval request awaiting an explicit chat response."""

    request_id: int | str
    method: str
    umo: str
    event: AstrMessageEvent
    details: dict[str, Any]
    timer: asyncio.Task | None = None


class CodexPlugin(Star):
    """Expose Codex app-server threads as continuous private-chat sessions."""

    _HIDDEN_ACTIVITY_METHODS = frozenset(
        {
            "thread/status/changed",
            "thread/tokenUsage/updated",
            "turn/diff/updated",
            "turn/started",
            "turn/completed",
        }
    )
    _COALESCED_ACTIVITY_METHODS = frozenset(
        {
            "item/agentMessage/delta",
            "item/commandExecution/outputDelta",
            "item/fileChange/outputDelta",
            "item/mcpToolCall/progress",
            "item/plan/delta",
            "item/reasoning/summaryTextDelta",
        }
    )

    def __init__(self, context: Context, config: AstrBotConfig | dict | None = None):
        super().__init__(context)
        settings = dict(config or {})
        unknown = set(settings) - {
            "codex_command",
            "approval_timeout",
            "send_final_report",
            "allow_group_chat",
        }
        if unknown:
            raise ValueError("未知插件配置：" + ", ".join(sorted(unknown)))
        self.codex_command = str(settings.get("codex_command", "codex")).strip()
        self.approval_timeout = settings.get("approval_timeout", 180)
        self.send_final_report = settings.get("send_final_report", False)
        self.allow_group_chat = settings.get("allow_group_chat", False)
        if (
            type(self.approval_timeout) is not int
            or not 30 <= self.approval_timeout <= 900
        ):
            raise ValueError("approval_timeout 必须是 30～900 之间的整数。")
        if type(self.send_final_report) is not bool:
            raise ValueError("send_final_report 必须是布尔值。")
        if type(self.allow_group_chat) is not bool:
            raise ValueError("allow_group_chat 必须是布尔值。")

        self.data_dir = StarTools.get_data_dir("astrbot_plugin_codex")
        self.bindings_file = self.data_dir / "bindings.json"
        self.bindings: dict[str, CodexBinding] = {}
        self.active_turns: dict[str, ActiveTurn] = {}
        self.pending_approvals: dict[int | str, PendingApproval] = {}
        self.client: AppServerClient | None = None
        self.loaded_threads: set[str] = set()
        self.background_tasks: set[asyncio.Task] = set()
        self._stopping = False

    async def initialize(self) -> None:
        """Load saved chat-to-thread bindings without starting Codex eagerly."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        try:
            raw = json.loads(self.bindings_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("bindings.json must contain an object")
            for umo, value in raw.items():
                if not isinstance(umo, str) or not isinstance(value, dict):
                    continue
                thread_id = value.get("thread_id")
                cwd = value.get("cwd")
                if isinstance(thread_id, str) and isinstance(cwd, str):
                    self.bindings[umo] = CodexBinding(
                        umo=umo,
                        thread_id=thread_id,
                        cwd=cwd,
                        model=(
                            value.get("model")
                            if isinstance(value.get("model"), str)
                            else None
                        ),
                        reasoning_effort=(
                            value.get("reasoning_effort")
                            if isinstance(value.get("reasoning_effort"), str)
                            else None
                        ),
                        enabled=False,
                    )
        except FileNotFoundError:
            pass
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            self.logger.warning("无法读取 Codex 会话绑定：%s", exc)

    async def terminate(self) -> None:
        """Leave continuous chat, stop pending work, and close Codex."""
        self._stopping = True
        connected_sessions = [
            binding.umo for binding in self.bindings.values() if binding.enabled
        ]
        if connected_sessions:
            for binding in self.bindings.values():
                binding.enabled = False
            try:
                self._save_bindings()
            except OSError as exc:
                self.logger.warning("无法保存关闭后的 Codex 会话状态：%s", exc)
        for approval in list(self.pending_approvals.values()):
            if approval.timer:
                approval.timer.cancel()
        self.pending_approvals.clear()
        for state in self.active_turns.values():
            state.suppress_reply = True
            if not state.completed.done():
                state.completed.set_result(
                    {"status": "failed", "error": {"message": "AstrBot 正在关闭。"}}
                )
        for task in list(self.background_tasks):
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        if self.client:
            await self.client.close()
            self.client = None
        for umo in connected_sessions:
            try:
                delivered = await self.context.send_message(
                    umo,
                    MessageChain().message(
                        "Codex 插件已关闭，当前聊天已断开连接。"
                        "插件重新启用后，发送 /codex 可重新连接。"
                    ),
                )
                if not delivered:
                    self.logger.warning(
                        "无法通知 Codex 会话已断开，未找到平台：%s", umo
                    )
            except Exception as exc:
                self.logger.warning("无法通知 Codex 会话已断开（%s）：%s", umo, exc)

    def _save_bindings(self) -> None:
        """Write bindings atomically so a restart cannot leave partial JSON."""
        payload = {
            umo: {
                "thread_id": binding.thread_id,
                "cwd": binding.cwd,
                "model": binding.model,
                "reasoning_effort": binding.reasoning_effort,
                "enabled": binding.enabled,
            }
            for umo, binding in self.bindings.items()
        }
        temporary = self.bindings_file.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temporary.replace(self.bindings_file)

    async def _ensure_client(self) -> AppServerClient:
        if self._stopping:
            raise CodexAppServerError("Codex 插件正在关闭。")
        if self.client is None:
            self.client = AppServerClient(
                self.codex_command,
                on_notification=self._on_notification,
                on_server_request=self._on_server_request,
                on_closed=self._on_client_closed,
                logger=self.logger,
            )
        client = self.client
        try:
            await client.start()
        except Exception:
            if self.client is client:
                self.client = None
            await client.close()
            raise
        return client

    @staticmethod
    def _session_key(event: AstrMessageEvent) -> str:
        if event.get_message_type() == MessageType.GROUP_MESSAGE:
            return (
                f"{event.get_platform_id()}:{MessageType.GROUP_MESSAGE.value}:"
                f"{event.get_group_id()}"
            )
        return event.unified_msg_origin

    async def _workspace(self, event: AstrMessageEvent) -> Path:
        path = (
            Path(await resolve_workspace_root_for_umo(self._session_key(event)))
            .expanduser()
            .resolve(strict=False)
        )
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise CodexAppServerError(f"会话工作目录不可用：{path}")
        return path

    def _authorized(self, event: AstrMessageEvent) -> str | None:
        if event.get_message_type() == MessageType.GROUP_MESSAGE:
            if not self.allow_group_chat:
                return "Codex 群聊连接未开启，请在插件配置中启用。"
            if not event.get_group_id():
                return "无法识别群聊 ID，不能连接 Codex。"
        elif not event.is_private_chat():
            return "Codex 插件只接受管理员私聊或群聊。"
        if not event.is_admin():
            return "只有 AstrBot 管理员可以使用 Codex 插件。"
        return None

    async def _create_binding(
        self, event: AstrMessageEvent, previous: CodexBinding | None = None
    ) -> CodexBinding:
        client = await self._ensure_client()
        cwd = await self._workspace(event)
        if previous:
            state = self.active_turns.get(previous.thread_id)
            if state:
                state.suppress_reply = True
                await self._interrupt(previous, state)
            await self._decline_approvals(previous.thread_id)
        response = await client.request(
            "thread/start",
            {"cwd": str(cwd)},
        )
        thread = response.get("thread", {}) if isinstance(response, dict) else {}
        thread_id = thread.get("id")
        if not isinstance(thread_id, str) or not thread_id:
            raise CodexAppServerError("Codex 未返回有效的线程 ID。")
        binding = CodexBinding(
            umo=self._session_key(event),
            thread_id=thread_id,
            cwd=str(cwd),
            model=self._response_model(response, thread),
            reasoning_effort=self._response_reasoning_effort(response, thread),
        )
        self.loaded_threads.add(thread_id)
        self.bindings[binding.umo] = binding
        self._save_bindings()
        return binding

    async def _enable_binding(self, event: AstrMessageEvent) -> str:
        umo = self._session_key(event)
        cwd = await self._workspace(event)
        binding = self.bindings.get(umo)
        if binding is None:
            binding = await self._create_binding(event)
            return (
                f"Codex 已连接。\n模型：{self._model_description(binding)}\n"
                f"工作目录：{binding.cwd}\n"
                f"线程：{binding.thread_id}"
            )

        async with binding.lock:
            active = self.active_turns.get(binding.thread_id)
            if active and not active.completed.done():
                active.event = event
                if binding.cwd != str(cwd):
                    client = await self._ensure_client()
                    await client.request(
                        "thread/settings/update",
                        {"threadId": binding.thread_id, "cwd": str(cwd)},
                    )
                binding.cwd = str(cwd)
                binding.enabled = True
                await self._refresh_model(binding)
                self._save_bindings()
                return (
                    f"Codex 已连接，当前任务仍在运行。\n"
                    f"模型：{self._model_description(binding)}\n工作目录：{binding.cwd}"
                )
            await self._resume_binding(binding, cwd=str(cwd))
            binding.cwd = str(cwd)
            binding.enabled = True
            await self._refresh_model(binding)
            self._save_bindings()
        return (
            f"Codex 已连接。\n模型：{self._model_description(binding)}\n"
            f"工作目录：{binding.cwd}\n"
            f"线程：{binding.thread_id}"
        )

    @staticmethod
    def _response_model(response: Any, thread: Any) -> str | None:
        if isinstance(response, dict):
            model = response.get("model")
            if isinstance(model, str) and model:
                return model
        if isinstance(thread, dict):
            model = thread.get("model")
            if isinstance(model, str) and model:
                return model
        return None

    @staticmethod
    def _response_reasoning_effort(response: Any, thread: Any) -> str | None:
        if isinstance(response, dict) and "reasoningEffort" in response:
            effort = response.get("reasoningEffort")
            if isinstance(effort, str) and effort:
                return effort
            return None
        if isinstance(thread, dict) and "reasoningEffort" in thread:
            effort = thread.get("reasoningEffort")
            if isinstance(effort, str) and effort:
                return effort
        return None

    @staticmethod
    def _has_response_reasoning_effort(response: Any, thread: Any) -> bool:
        return (isinstance(response, dict) and "reasoningEffort" in response) or (
            isinstance(thread, dict) and "reasoningEffort" in thread
        )

    @staticmethod
    def _model_description(binding: CodexBinding) -> str:
        model = binding.model or "未知"
        if binding.reasoning_effort:
            return f"{model} {binding.reasoning_effort}"
        return model

    async def _refresh_model(self, binding: CodexBinding) -> None:
        try:
            client = await self._ensure_client()
            response = await client.request(
                "thread/read", {"threadId": binding.thread_id, "includeTurns": False}
            )
        except (CodexAppServerError, OSError) as exc:
            self.logger.warning("无法刷新 Codex 模型信息：%s", exc)
            return
        thread = response.get("thread", {}) if isinstance(response, dict) else {}
        binding.model = self._response_model(response, thread) or binding.model
        if self._has_response_reasoning_effort(response, thread):
            binding.reasoning_effort = self._response_reasoning_effort(response, thread)

    async def _require_binding(self, event: AstrMessageEvent) -> CodexBinding:
        binding = self.bindings.get(self._session_key(event))
        if binding is None:
            raise CodexAppServerError("请先发送 /codex 连接 Codex。")
        async with binding.lock:
            await self._resume_binding(binding)
        return binding

    async def _resume_binding(
        self, binding: CodexBinding, *, cwd: str | None = None
    ) -> None:
        if binding.thread_id in self.loaded_threads:
            return
        client = await self._ensure_client()
        try:
            response = await client.request(
                "thread/resume",
                {
                    "threadId": binding.thread_id,
                    "cwd": cwd or binding.cwd,
                },
            )
        except CodexAppServerError as exc:
            if "no rollout found" not in str(exc).lower():
                raise
            response = await client.request(
                "thread/start",
                {"cwd": cwd or binding.cwd},
            )
            thread = response.get("thread", {}) if isinstance(response, dict) else {}
            thread_id = thread.get("id") if isinstance(thread, dict) else None
            if not isinstance(thread_id, str) or not thread_id:
                raise CodexAppServerError("Codex 未返回有效的线程 ID。") from exc
            binding.thread_id = thread_id
            binding.cwd = cwd or binding.cwd
            binding.model = self._response_model(response, thread)
            binding.reasoning_effort = self._response_reasoning_effort(response, thread)
            self.loaded_threads.add(thread_id)
            self._save_bindings()
            return
        thread = response.get("thread", {}) if isinstance(response, dict) else {}
        binding.model = self._response_model(response, thread) or binding.model
        if self._has_response_reasoning_effort(response, thread):
            binding.reasoning_effort = self._response_reasoning_effort(response, thread)
        self.loaded_threads.add(binding.thread_id)
        self._save_bindings()

    @filter.command("codex", priority=1000)
    async def codex(
        self,
        event: AstrMessageEvent,
        action: str = "",
        argument: GreedyStr = "",
    ):
        """Enable a Codex conversation or run a mapped app-server command."""
        event.stop_event()
        denied = self._authorized(event)
        if denied:
            return event.plain_result(denied)
        action = action.lower().strip()
        argument = str(argument).strip()
        try:
            if not action:
                result = await self._enable_binding(event)
            elif action == "exit":
                binding = await self._require_binding(event)
                async with binding.lock:
                    binding.enabled = False
                    self._save_bindings()
                result = "当前聊天已断开 Codex。线程和工作目录已保留，发送 /codex 可重新连接。"
            elif action == "close":
                result = await self._close_binding(event)
            elif action == "yes":
                result = await self._answer_approval(event, approve=True)
            elif action == "no":
                result = await self._answer_approval(event, approve=False)
            elif action == "stop":
                binding = await self._require_binding(event)
                async with binding.lock:
                    state = self.active_turns.get(binding.thread_id)
                    if not state or state.completed.done():
                        result = "当前没有正在运行的 Codex 任务。"
                    else:
                        interrupted = await self._interrupt(binding, state)
                        if interrupted:
                            return
                        result = "暂未取得运行中任务的 ID，请稍后重试 /codex stop。"
            elif action == "status":
                binding = self.bindings.get(self._session_key(event))
                if binding is None:
                    raise CodexAppServerError("请先发送 /codex 连接 Codex。")
                state = self.active_turns.get(binding.thread_id)
                running = bool(state and not state.completed.done())
                activity = (
                    list(state.active_items.values())[-1]
                    if state and state.active_items
                    else state.current_activity
                    if state
                    else "无运行中的任务"
                )
                activity_count = (
                    f"{len(self._activity_groups(state.activity_events))} 项"
                    if state and self.send_final_report
                    else "未收集（报告已关闭）"
                    if state
                    else "0 项"
                )
                work_time = (
                    self._elapsed_text(state.started_at)
                    if running and state
                    else "当前无运行中的任务"
                )
                result = (
                    f"连接状态：{'已连接' if binding.enabled else '未连接'}\n"
                    f"任务：{'运行中' if running else '空闲'}\n"
                    f"本轮工作时间：{work_time}\n"
                    f"当前活动：{activity}\n"
                    f"活动记录：{activity_count}\n"
                    f"模型：{self._model_description(binding)}\n"
                    f"工作目录：{binding.cwd}\n线程：{binding.thread_id}"
                )
            elif action == "pwd":
                binding = self.bindings.get(self._session_key(event))
                if binding is None:
                    raise CodexAppServerError("请先发送 /codex 连接 Codex。")
                result = binding.cwd
            elif action == "new":
                previous = self.bindings.get(self._session_key(event))
                if previous:
                    async with previous.lock:
                        binding = await self._create_binding(event, previous)
                else:
                    binding = await self._create_binding(event)
                result = (
                    f"已切换到新的 Codex 线程。\n工作目录：{binding.cwd}\n"
                    f"线程：{binding.thread_id}"
                )
            elif action == "model":
                binding = await self._require_binding(event)
                async with binding.lock:
                    state = self.active_turns.get(binding.thread_id)
                    if state and not state.completed.done():
                        raise CodexAppServerError("当前任务运行中，暂不能切换模型。")
                    if not argument:
                        result = await self._list_models()
                    else:
                        parts = argument.split()
                        if len(parts) > 2:
                            raise CodexAppServerError(
                                "model 用法：/codex model <模型ID> [思考等级]。"
                            )
                        model = await self._find_model(parts[0])
                        model_id = str(model.get("model") or model.get("id"))
                        efforts = self._model_efforts(model)
                        requested_effort = parts[1].lower() if len(parts) == 2 else ""
                        if requested_effort and requested_effort not in efforts:
                            choices = ", ".join(efforts) or "该模型未提供可选等级"
                            raise CodexAppServerError(
                                f"模型 {model_id} 不支持思考等级 {requested_effort}。"
                                f"可选等级：{choices}。"
                            )
                        if not requested_effort:
                            if binding.reasoning_effort in efforts:
                                requested_effort = binding.reasoning_effort or ""
                            else:
                                default_effort = model.get("defaultReasoningEffort")
                                if isinstance(default_effort, str):
                                    requested_effort = default_effort

                        client = await self._ensure_client()
                        params = {"threadId": binding.thread_id, "model": model_id}
                        if requested_effort:
                            params["effort"] = requested_effort
                        await client.request("thread/settings/update", params)
                        binding.model = model_id
                        binding.reasoning_effort = requested_effort or None
                        self._save_bindings()
                        result = (
                            f"当前线程的模型已设为 {self._model_description(binding)}。"
                        )
            elif action == "effort":
                binding = await self._require_binding(event)
                async with binding.lock:
                    state = self.active_turns.get(binding.thread_id)
                    if state and not state.completed.done():
                        raise CodexAppServerError(
                            "当前任务运行中，暂不能切换思考等级。"
                        )
                    if not binding.model:
                        raise CodexAppServerError("当前线程没有可识别的模型。")
                    model = await self._find_model(binding.model)
                    efforts = self._model_efforts(model)
                    if not argument:
                        if not efforts:
                            raise CodexAppServerError("当前模型没有可选的思考等级。")
                        default_effort = model.get("defaultReasoningEffort", "未知")
                        result = (
                            f"模型 {binding.model} 可选思考等级：{', '.join(efforts)}\n"
                            f"默认：{default_effort}\n"
                            f"当前：{binding.reasoning_effort or '默认'}"
                        )
                    else:
                        requested_effort = argument.lower()
                        if requested_effort not in efforts:
                            choices = ", ".join(efforts) or "该模型未提供可选等级"
                            raise CodexAppServerError(
                                f"模型 {binding.model} 不支持思考等级 {requested_effort}。"
                                f"可选等级：{choices}。"
                            )
                        client = await self._ensure_client()
                        await client.request(
                            "thread/settings/update",
                            {"threadId": binding.thread_id, "effort": requested_effort},
                        )
                        binding.reasoning_effort = requested_effort
                        self._save_bindings()
                        result = (
                            f"当前线程的思考等级已设为 "
                            f"{self._model_description(binding)}。"
                        )
            elif action == "compact":
                binding = await self._require_binding(event)
                async with binding.lock:
                    self._ensure_idle(binding)
                    await self._launch_turn(
                        event,
                        binding,
                        "thread/compact/start",
                        {"threadId": binding.thread_id},
                        success_message="Codex 上下文压缩已完成。",
                    )
                result = "已请求 Codex 压缩上下文，完成后会在此会话回复。"
            elif action == "review":
                binding = await self._require_binding(event)
                target = self._review_target(argument)
                async with binding.lock:
                    self._ensure_idle(binding)
                    await self._launch_turn(
                        event,
                        binding,
                        "review/start",
                        {"threadId": binding.thread_id, "target": target},
                    )
                result = "已启动 Codex 代码审查，完成后会在此会话回复。"
            elif action == "help":
                result = self._help_text()
            else:
                result = (
                    f"暂不支持 Codex 命令 /{action}。可用命令："
                    "help、exit、close、yes、no、stop、status、pwd、new、compact、review、model、effort。"
                    "要把斜杠文本作为普通任务，请在消息开头加 //。"
                )
        except (CodexAppServerError, OSError, ValueError) as exc:
            result = f"Codex 操作失败：{exc}"
        except Exception:
            self.logger.exception("Codex command failed")
            result = "Codex 操作失败，请查看 AstrBot 日志。"
        return event.plain_result(result)

    @filter.event_message_type(filter.EventMessageType.ALL, priority=900)
    async def handle_codex_chat(self, event: AstrMessageEvent) -> None:
        """Forward ordinary text to the enabled Codex thread for this UMO."""
        binding = self.bindings.get(self._session_key(event))
        if not binding or not binding.enabled or self._authorized(event):
            return
        text = event.get_message_str().strip()
        if not text:
            return
        if text.startswith("/codex") or (
            text.startswith("/") and not text.startswith("//")
        ):
            return
        if text.startswith("//"):
            text = text[1:]
        event.stop_event()
        event.should_call_llm(False)
        try:
            async with binding.lock:
                if self.bindings.get(binding.umo) is not binding or not binding.enabled:
                    return
                await self._resume_binding(binding)
                active = self.active_turns.get(binding.thread_id)
                if active and active.completed.done():
                    self.active_turns.pop(binding.thread_id, None)
                    active = None
                if active:
                    if active.turn_id is None:
                        await asyncio.wait_for(active.turn_started.wait(), timeout=10)
                    active.event = event
                    client = await self._ensure_client()
                    await client.request(
                        "turn/steer",
                        {
                            "threadId": binding.thread_id,
                            "expectedTurnId": active.turn_id,
                            "input": [self._text_input(text)],
                        },
                    )
                else:
                    await self._launch_turn(
                        event,
                        binding,
                        "turn/start",
                        {
                            "threadId": binding.thread_id,
                            "input": [self._text_input(text)],
                        },
                    )
        except Exception as exc:
            self.logger.warning("Unable to forward message to Codex: %s", exc)
            await self._send_text(event, f"无法发送给 Codex：{exc}")

    @staticmethod
    def _text_input(text: str) -> dict[str, Any]:
        return {"type": "text", "text": text, "textElements": []}

    @staticmethod
    def _elapsed_text(started_at: float) -> str:
        elapsed = max(0, int(monotonic() - started_at))
        return f"{elapsed // 60} 分 {elapsed % 60} 秒"

    def _ensure_idle(self, binding: CodexBinding) -> None:
        state = self.active_turns.get(binding.thread_id)
        if state and not state.completed.done():
            raise CodexAppServerError("当前任务运行中，请先使用 /codex stop。")

    async def _launch_turn(
        self,
        event: AstrMessageEvent,
        binding: CodexBinding,
        method: str,
        params: dict[str, Any],
        *,
        success_message: str | None = None,
    ) -> ActiveTurn:
        request_text = ""
        request_truncated = False
        if self.send_final_report:
            request_text = event.get_message_str().strip()
            inputs = params.get("input")
            if isinstance(inputs, list):
                texts = [
                    item["text"]
                    for item in inputs
                    if isinstance(item, dict) and isinstance(item.get("text"), str)
                ]
                if texts:
                    request_text = "\n".join(texts).strip()
            request_text, request_truncated = self._clip_report_text(
                request_text, 4_000
            )
        state = ActiveTurn(
            binding=binding,
            thread_id=binding.thread_id,
            event=event,
            completed=asyncio.get_running_loop().create_future(),
            request_text=request_text,
            success_message=success_message,
            activity_truncated=request_truncated,
        )
        self.active_turns[binding.thread_id] = state
        task = self._track_task(self._run_turn(binding, state, method, params))
        state.task = task
        return state

    def _track_task(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task

    async def _run_turn(
        self,
        binding: CodexBinding,
        state: ActiveTurn,
        method: str,
        params: dict[str, Any],
    ) -> None:
        try:
            client = await self._ensure_client()
            await self._resume_binding(binding)
            if state.thread_id != binding.thread_id:
                self.active_turns.pop(state.thread_id, None)
                state.thread_id = binding.thread_id
                self.active_turns[state.thread_id] = state
                params = {**params, "threadId": binding.thread_id}
            response = await client.request(method, params)
            turn = response.get("turn", {}) if isinstance(response, dict) else {}
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if isinstance(turn_id, str):
                state.turn_id = turn_id
                state.turn_started.set()
                if state.suppress_reply:
                    await self._interrupt(binding, state)
            outcome = await state.completed
            if state.suppress_reply:
                return
            if state.delivery_task:
                await asyncio.gather(state.delivery_task, return_exceptions=True)
            for reply in self._remaining_reply_texts(state, outcome):
                await self._send_codex_reply(state.event, reply)
                state.reply_sent = True
            status = outcome.get("status") if isinstance(outcome, dict) else None
            error = outcome.get("error") if isinstance(outcome, dict) else None
            terminal_message = None
            if error:
                error_text = (
                    error.get("message") if isinstance(error, dict) else str(error)
                )
                terminal_message = f"Codex 任务失败：{error_text}"
                report_status = "失败"
            elif status == "interrupted":
                terminal_message = "Codex 任务已中断。"
                report_status = "已中断"
            elif status == "failed":
                terminal_message = "Codex 任务失败，请查看 AstrBot 日志。"
                report_status = "失败"
            else:
                report_status = "已完成" if status == "completed" else "已结束"
                if state.success_message:
                    terminal_message = state.success_message
                elif not state.reply_sent:
                    terminal_message = "Codex 已完成任务，但没有返回文本。"
            if terminal_message:
                await self._send_codex_reply(state.event, terminal_message)
            state.current_activity = report_status
            if self.send_final_report and self._should_send_final_report(
                state, status=report_status
            ):
                await self._send_final_activity_report(state, status=report_status)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not state.suppress_reply:
                state.current_activity = "失败"
                await self._send_codex_reply(state.event, f"Codex 任务失败：{exc}")
                if self.send_final_report:
                    await self._send_final_activity_report(
                        state, status="失败", error=str(exc)
                    )
        finally:
            if self.active_turns.get(state.thread_id) is state:
                self.active_turns.pop(state.thread_id, None)

    def _queue_codex_reply(self, state: ActiveTurn, message: str) -> None:
        if state.suppress_reply or not message.strip():
            return
        previous = state.delivery_task

        async def deliver() -> None:
            if previous:
                await asyncio.gather(previous, return_exceptions=True)
            if not state.suppress_reply:
                await self._send_codex_reply(state.event, message)

        state.delivery_task = self._track_task(deliver())
        state.reply_sent = True
        state.sent_message_texts.add(message)

    @staticmethod
    def _remaining_reply_texts(state: ActiveTurn, outcome: Any) -> list[str]:
        replies: list[str] = []
        turn = outcome.get("turn", outcome) if isinstance(outcome, dict) else {}
        items = turn.get("items") if isinstance(turn, dict) else None
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict) or item.get("type") != "agentMessage":
                    continue
                item_id = item.get("id")
                if isinstance(item_id, str) and item_id in state.sent_message_ids:
                    state.message_deltas.pop(item_id, None)
                    continue
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    if (
                        not isinstance(item_id, str)
                        and text in state.sent_message_texts
                    ):
                        continue
                    replies.append(text)
                    if isinstance(item_id, str):
                        state.message_deltas.pop(item_id, None)
        for item_id, deltas in state.message_deltas.items():
            if item_id not in state.sent_message_ids:
                text = "".join(deltas)
                if text.strip():
                    replies.append(text)
        unassigned = "".join(state.unassigned_deltas)
        if unassigned.strip() and unassigned not in state.sent_message_texts:
            replies.append(unassigned)
        return replies

    @staticmethod
    def _activity_title(method: str, params: dict[str, Any]) -> str:
        item = params.get("item")
        if isinstance(item, dict):
            kind = item.get("type")
            if kind == "commandExecution":
                command = str(item.get("command") or "命令执行")
                return "执行命令：" + command.replace("\n", " ")[:180]
            if kind == "mcpToolCall":
                return f"MCP 工具：{item.get('server', '?')}/{item.get('tool', '?')}"
            if kind == "fileChange":
                return f"文件变更：{len(item.get('changes', []))} 项"
            if kind == "agentMessage":
                return "Codex 回复"
            if kind == "reasoning":
                return "推理摘要"
            if kind == "plan":
                return "更新执行计划"
            return f"Codex 活动：{kind or '未知项目'}"
        if method == "item/commandExecution/outputDelta":
            return "命令输出"
        if method == "item/mcpToolCall/progress":
            message = str(params.get("message") or "MCP 工具处理中")
            return "MCP 工具进度：" + message[:160]
        if method.startswith("serverRequest/"):
            request_method = method.removeprefix("serverRequest/")
            if request_method == "item/commandExecution/requestApproval":
                return "等待命令执行审批"
            if request_method == "item/fileChange/requestApproval":
                return "等待文件修改审批"
            if request_method == "item/permissions/requestApproval":
                return "处理额外权限请求"
            if request_method == "resolved":
                decision = str(params.get("decision") or "已处理")
                return "审批结果：" + decision
            return "权限审批：" + request_method
        if method == "item/agentMessage/delta":
            return "Codex 回复"
        if method.startswith("item/reasoning/"):
            return "推理摘要更新"
        if method.startswith("turn/"):
            return "Codex 任务状态：" + method.removeprefix("turn/")
        return method

    @staticmethod
    def _clip_report_text(value: str, limit: int) -> tuple[str, bool]:
        if len(value) <= limit:
            return value, False
        if limit <= len(_REPORT_TRUNCATION):
            return _REPORT_TRUNCATION.strip(), True
        available = limit - len(_REPORT_TRUNCATION)
        head = available // 4
        tail = available - head
        return value[:head] + _REPORT_TRUNCATION + value[-tail:], True

    @classmethod
    def _activity_snapshot(cls, params: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        remaining_text = _REPORT_EVENT_TEXT_LIMIT
        remaining_nodes = _REPORT_NODE_LIMIT
        truncated = False

        def copy(value: Any, depth: int = 0) -> Any:
            nonlocal remaining_text, remaining_nodes, truncated
            if depth >= 16 or remaining_nodes <= 0:
                truncated = True
                return _REPORT_TRUNCATION.strip()
            remaining_nodes -= 1
            if isinstance(value, str):
                limit = min(_REPORT_FIELD_TEXT_LIMIT, remaining_text)
                result, clipped = cls._clip_report_text(value, limit)
                remaining_text -= min(len(value), limit)
                truncated |= clipped
                return result
            if isinstance(value, dict):
                result = {}
                for index, (key, item) in enumerate(value.items()):
                    if index >= _REPORT_CONTAINER_LIMIT or remaining_nodes <= 0:
                        truncated = True
                        result["…"] = _REPORT_TRUNCATION.strip()
                        break
                    result[str(key)] = copy(item, depth + 1)
                return result
            if isinstance(value, (list, tuple)):
                result = []
                for index, item in enumerate(value):
                    if index >= _REPORT_CONTAINER_LIMIT or remaining_nodes <= 0:
                        truncated = True
                        result.append(_REPORT_TRUNCATION.strip())
                        break
                    result.append(copy(item, depth + 1))
                return result
            if value is None or isinstance(value, (bool, int, float)):
                return value
            return copy(str(value), depth + 1)

        return copy(params), truncated

    def _record_activity(
        self, state: ActiveTurn, method: str, params: dict[str, Any]
    ) -> None:
        if not self.send_final_report or method in self._HIDDEN_ACTIVITY_METHODS:
            return
        raw_item = params.get("item")
        if isinstance(raw_item, dict) and raw_item.get("type") == "userMessage":
            return
        if method.startswith(
            (
                "serverRequest/",
                "item/commandExecution/",
                "item/fileChange/",
                "item/mcpToolCall/",
                "item/plan/",
            )
        ) or (
            isinstance(raw_item, dict)
            and raw_item.get("type") not in {"userMessage", "agentMessage", "reasoning"}
        ):
            state.had_reportable_activity = True
        snapshot, truncated = self._activity_snapshot(params)
        state.activity_truncated |= truncated
        item = snapshot.get("item")
        item_id = (
            item.get("id")
            if isinstance(item, dict) and isinstance(item.get("id"), str)
            else snapshot.get("itemId")
        )
        item_id = item_id if isinstance(item_id, str) else None
        if method in self._COALESCED_ACTIVITY_METHODS and state.activity_events:
            previous = state.activity_events[-1]
            if previous.get("method") == method and previous.get("item_id") == item_id:
                previous_params = previous.get("params")
                if isinstance(previous_params, dict):
                    if isinstance(snapshot.get("delta"), str):
                        previous_params["delta"], truncated = self._clip_report_text(
                            str(previous_params.get("delta") or "") + snapshot["delta"],
                            _REPORT_FIELD_TEXT_LIMIT,
                        )
                        state.activity_truncated |= truncated
                    elif isinstance(snapshot.get("message"), str):
                        previous_params["message"] = snapshot["message"]
                    else:
                        previous_params.update(snapshot)
                    previous["update_count"] = previous.get("update_count", 1) + 1
                    previous["time"] = datetime.now().astimezone().strftime("%H:%M:%S")
                    return
        event = {
            "sequence": len(state.activity_events) + 1,
            "time": datetime.now().astimezone().strftime("%H:%M:%S"),
            "method": method,
            "item_id": item_id,
            "title": self._activity_title(method, snapshot),
            "params": snapshot,
            "update_count": 1,
        }
        if len(state.activity_events) == _REPORT_EVENT_LIMIT:
            state.activity_truncated = True
        state.activity_events.append(event)

    @staticmethod
    def _notification_turn_id(method: str, params: dict[str, Any]) -> str | None:
        if method in {"turn/started", "turn/completed"}:
            turn = params.get("turn")
            if isinstance(turn, dict) and isinstance(turn.get("id"), str):
                return turn["id"]
            return None
        turn_id = params.get("turnId")
        return turn_id if isinstance(turn_id, str) else None

    def _activity_for_item(
        self, state: ActiveTurn, method: str, params: dict[str, Any]
    ) -> None:
        if method in self._HIDDEN_ACTIVITY_METHODS and method not in {
            "turn/started",
            "turn/completed",
        }:
            return
        item = params.get("item")
        if isinstance(item, dict) and item.get("type") == "userMessage":
            return
        if isinstance(item, dict) and item.get("type") == "reasoning":
            if method == "item/completed":
                state.current_activity = "等待 Codex 继续工作"
            return
        item_id = None
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            item_id = item["id"]
        if not item_id and isinstance(params.get("itemId"), str):
            item_id = params["itemId"]
        if method == "item/started" and item_id:
            state.active_items[item_id] = self._activity_title(method, params)
        elif method == "item/completed" and item_id:
            state.active_items.pop(item_id, None)
        if method == "turn/started":
            state.current_activity = "Codex 任务已开始"
        elif method == "turn/completed":
            state.current_activity = "Codex 任务已完成"
        elif method == "item/completed":
            state.current_activity = "等待 Codex 继续工作"
        elif method == "item/reasoning/summaryTextDelta":
            delta = params.get("delta")
            if isinstance(delta, str) and delta.strip():
                state.current_activity = "推理摘要：" + " ".join(delta.split())[:160]
            else:
                state.current_activity = "推理摘要更新"
        else:
            state.current_activity = self._activity_title(method, params)

    @staticmethod
    def _agent_message_text(events: list[dict[str, Any]]) -> str:
        completed_text = ""
        deltas = []
        for event in events:
            params = event.get("params")
            if not isinstance(params, dict):
                continue
            if event.get("method") == "item/agentMessage/delta":
                delta = params.get("delta")
                if isinstance(delta, str):
                    deltas.append(delta)
            elif event.get("method") == "item/completed":
                item = params.get("item")
                if isinstance(item, dict) and item.get("type") == "agentMessage":
                    value = item.get("text")
                    if isinstance(value, str) and value.strip():
                        completed_text = value
        text = completed_text or "".join(deltas)
        return text if text.strip() else ""

    @staticmethod
    def _activity_preview(events: list[dict[str, Any]], limit: int | None = 420) -> str:
        """Extract human-readable streaming text without replacing raw events."""
        chunks: list[str] = []
        seen: set[str] = set()
        for event in events:
            method = event.get("method")
            params = event.get("params")
            if not isinstance(params, dict):
                continue
            text: str | None = None
            if method in {
                "item/reasoning/summaryTextDelta",
                "item/plan/delta",
                "item/commandExecution/outputDelta",
                "item/fileChange/outputDelta",
            }:
                value = params.get("delta")
                if isinstance(value, str):
                    text = value
            elif method == "item/mcpToolCall/progress":
                value = params.get("message")
                if isinstance(value, str):
                    text = value
            elif method in {"item/started", "item/completed"}:
                item = params.get("item")
                if isinstance(item, dict):
                    if item.get("type") == "reasoning":
                        summary = item.get("summary")
                        if isinstance(summary, list):
                            value = "\n".join(
                                part for part in summary if isinstance(part, str)
                            )
                            if value:
                                text = value
                    elif item.get("type") == "plan":
                        value = item.get("text")
                        if isinstance(value, str):
                            text = value
            if not text or not text.strip():
                continue
            text = text.strip()
            if text in seen:
                continue
            seen.add(text)
            chunks.append(text)
        preview = "\n".join(chunks)
        if limit is not None and len(preview) > limit:
            return preview[: max(0, limit - 1)].rstrip() + "…"
        return preview

    @classmethod
    def _activity_groups(cls, events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for event in events:
            item_id = event.get("item_id")
            key = f"item:{item_id}" if item_id else f"method:{event['method']}"
            group = groups.get(key)
            if group is None:
                group = {
                    "title": event.get("title", event["method"]),
                    "first_time": event.get("time", ""),
                    "last_time": event.get("time", ""),
                    "events": [],
                    "kind": None,
                }
                groups[key] = group
            elif event["method"] in {"item/started", "item/completed"}:
                group["title"] = event.get("title", group["title"])
            params = event.get("params")
            item = params.get("item") if isinstance(params, dict) else None
            if isinstance(item, dict) and isinstance(item.get("type"), str):
                group["kind"] = item["type"]
            elif group["kind"] is None:
                if event["method"].startswith("item/reasoning/"):
                    group["kind"] = "reasoning"
                elif event["method"].startswith("item/agentMessage/"):
                    group["kind"] = "agentMessage"
            group["last_time"] = event.get("time", group["last_time"])
            group["events"].append(event)
        visible = []
        for group in groups.values():
            if group["kind"] == "reasoning" and not cls._activity_preview(
                group["events"], limit=None
            ):
                continue
            if group["kind"] == "agentMessage" and not cls._agent_message_text(
                group["events"]
            ):
                continue
            if all(
                event["method"] == "serverRequest/resolved"
                and not any(
                    event.get("params", {}).get(key)
                    for key in ("decision", "reason", "command")
                )
                for event in group["events"]
            ):
                continue
            visible.append(group)
        return visible

    @staticmethod
    def _activity_item(events: list[dict[str, Any]]) -> dict[str, Any]:
        for event in reversed(events):
            params = event.get("params")
            item = params.get("item") if isinstance(params, dict) else None
            if isinstance(item, dict):
                return item
        return {}

    @classmethod
    def _plain_result(cls, value: Any) -> str:
        if isinstance(value, str):
            if value.lstrip().startswith(("{", "[")):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    pass
                else:
                    if isinstance(parsed, (dict, list)):
                        return cls._plain_result(parsed)
            return value
        if isinstance(value, dict):
            if value.get("type") == "image":
                return "返回图片内容"
            if value.get("type") == "audio":
                return "返回音频内容"
            if isinstance(value.get("text"), str):
                return cls._plain_result(value["text"])
            content = value.get("content")
            if isinstance(content, list):
                parts = []
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    block_text = block.get("text")
                    if isinstance(block_text, str) and block_text.strip():
                        parts.append(cls._plain_result(block_text))
                    elif block.get("type") == "image":
                        parts.append("返回图片内容")
                    elif block.get("type") == "audio":
                        parts.append("返回音频内容")
                    elif block.get("type") == "resource_link":
                        name = block.get("title") or block.get("name") or "资源"
                        uri = block.get("uri") or ""
                        parts.append(f"{name}：{uri}")
                if parts:
                    return "\n\n".join(parts)
            if value.get("structuredContent") is not None:
                return cls._plain_result(value["structuredContent"])
            lines = []
            for key, item in value.items():
                if (
                    key.startswith("_")
                    or key
                    in {
                        "data",
                        "blob",
                        "base64",
                        "image_url",
                    }
                    or item is None
                ):
                    continue
                rendered = cls._plain_result(item)
                if rendered:
                    label = {"errors": "错误", "message": "消息", "output": "输出"}.get(
                        key, key
                    )
                    lines.append(f"{label}：{rendered}")
            return "\n".join(lines)
        if isinstance(value, list):
            return "\n".join(
                rendered for item in value if (rendered := cls._plain_result(item))
            )
        return "" if value is None else str(value)

    @staticmethod
    def _tool_error(result: Any) -> str:
        if not isinstance(result, dict):
            return ""
        content = result.get("content")
        if not isinstance(content, list):
            return ""
        errors = []
        for block in content:
            text = block.get("text") if isinstance(block, dict) else None
            if not isinstance(text, str):
                continue
            first_line = text.strip().splitlines()[0] if text.strip() else ""
            if first_line.startswith("Error:"):
                errors.append(first_line)
                continue
            if text.lstrip().startswith("{"):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict) and isinstance(parsed.get("errors"), list):
                    errors.extend(str(error) for error in parsed["errors"] if error)
        return errors[0] if errors else ""

    @classmethod
    def _activity_result(cls, group: dict[str, Any]) -> tuple[str, str, str]:
        events = group["events"]
        item = cls._activity_item(events)
        kind = group["kind"]
        status = str(item.get("status") or "")
        status = {
            "completed": "已完成",
            "failed": "失败",
            "inProgress": "未完成",
            "interrupted": "已中断",
        }.get(status, status)
        exit_code = item.get("exitCode")
        if kind == "commandExecution" and isinstance(exit_code, int):
            status = "成功" if exit_code == 0 else "失败"
            status += f" · 退出码 {exit_code}"
        duration = item.get("durationMs")
        if isinstance(duration, (int, float)):
            duration_text = (
                f"{duration / 1000:.1f} 秒"
                if duration >= 1000
                else f"{duration:g} 毫秒"
            )
            status = f"{status} · {duration_text}" if status else duration_text

        decisions = [
            event.get("params", {}).get("decision")
            for event in events
            if event.get("method") == "serverRequest/resolved"
        ]
        decision = next((value for value in reversed(decisions) if value), None)
        if decision:
            status = f"审批：{decision}"

        result = ""
        highlight = ""
        if kind == "commandExecution":
            output = item.get("aggregatedOutput")
            result = (
                output
                if isinstance(output, str)
                else "".join(
                    event.get("params", {}).get("delta", "")
                    for event in events
                    if event.get("method") == "item/commandExecution/outputDelta"
                )
            )
        elif kind == "mcpToolCall":
            tool_result = item.get("result")
            result = cls._plain_result(tool_result)
            highlight = cls._tool_error(tool_result)
            if highlight:
                status = "返回错误"
            if item.get("error"):
                highlight = cls._plain_result(item["error"])
                status = "失败"
        elif kind == "fileChange":
            changes = item.get("changes")
            if not isinstance(changes, list) or not any(
                isinstance(change, dict) and change.get("diff") for change in changes
            ):
                changes = next(
                    (
                        event.get("params", {}).get("changes")
                        for event in reversed(events)
                        if event.get("method") == "item/fileChange/patchUpdated"
                    ),
                    changes,
                )
            if isinstance(changes, list):
                lines = []
                for change in changes:
                    if not isinstance(change, dict):
                        lines.append(str(change))
                        continue
                    change_kind = change.get("kind")
                    if isinstance(change_kind, dict):
                        change_kind = change_kind.get("type")
                    change_kind = (
                        change_kind
                        if isinstance(change_kind, str)
                        else str(change_kind or "")
                    )
                    label = {
                        "add": "新增",
                        "update": "修改",
                        "delete": "删除",
                        "move": "移动",
                    }.get(change_kind, change_kind)
                    lines.append(
                        " ".join(
                            part
                            for part in (label, str(change.get("path") or ""))
                            if part
                        )
                    )
                    diff = change.get("diff")
                    if isinstance(diff, str) and diff.strip():
                        lines.append(diff.strip())
                result = "\n".join(lines)
            if not result or not any(
                isinstance(change, dict)
                and isinstance(change.get("diff"), str)
                and change["diff"].strip()
                for change in changes or []
            ):
                output = cls._activity_preview(events, limit=None)
                if output:
                    result = f"{result}\n{output}".strip()
        elif kind == "agentMessage":
            result = cls._agent_message_text(events)
        else:
            result = cls._activity_preview(events, limit=None)

        if not result:
            reason = next(
                (
                    event.get("params", {}).get("reason")
                    for event in events
                    if event.get("params", {}).get("reason")
                ),
                None,
            )
            if isinstance(reason, str):
                result = reason
        return status.strip(), result.strip(), highlight.strip()

    @staticmethod
    def _result_excerpt(value: str, limit: int = _REPORT_RESULT_PREVIEW_LIMIT) -> str:
        if len(value) <= limit:
            return value
        head = value[:200].rstrip()
        tail = value[-200:].lstrip()
        return f"{head}\n…（中间内容已折叠）…\n{tail}"

    @staticmethod
    def _display_command(command: str) -> str:
        value = command.strip()
        if not value.startswith('"'):
            return command
        end = value.find('"', 1)
        if end < 0:
            return command
        executable = value[1:end]
        if PureWindowsPath(executable).name.lower() not in {
            "pwsh.exe",
            "powershell.exe",
        }:
            return command
        flag, separator, script = value[end + 1 :].strip().partition(" ")
        if flag.lower() != "-command" or not separator or not script.strip():
            return command
        script = script.strip()
        if len(script) >= 2 and script[0] in "'\"" and script[-1] == script[0]:
            script = script[1:-1]
        return script

    @staticmethod
    def _should_send_final_report(state: ActiveTurn, *, status: str) -> bool:
        return status != "已完成" or state.had_reportable_activity

    def _activity_report_html(
        self,
        state: ActiveTurn,
        *,
        status: str,
        full: bool,
        error: str | None = None,
    ) -> str:
        groups = self._activity_groups(state.activity_events)
        visible_groups = groups if full else groups[-24:]

        cards = []
        for group in visible_groups:
            kind = group["kind"]
            if kind == "agentMessage":
                style_kind, result_label = "reply", ""
            elif kind == "commandExecution":
                style_kind, result_label = "command", "输出"
            elif kind == "mcpToolCall":
                style_kind, result_label = "tool", "工具结果"
            elif kind == "fileChange":
                style_kind, result_label = "file", ""
            elif kind == "reasoning":
                style_kind, result_label = "reasoning", ""
            elif kind == "plan":
                style_kind, result_label = "plan", ""
            elif any(
                event["method"].startswith("serverRequest/")
                for event in group["events"]
            ):
                style_kind, result_label = "approval", "说明"
            else:
                style_kind, result_label = "other", "内容"
            title = html.escape(
                "执行命令" if kind == "commandExecution" else str(group["title"])
            )
            time_label = html.escape(
                str(group["first_time"])
                if group["first_time"] == group["last_time"]
                else f"{group['first_time']} - {group['last_time']}"
            )
            command_block = ""
            if kind == "commandExecution":
                command = self._activity_item(group["events"]).get("command")
                if isinstance(command, str) and command.strip():
                    command_block = (
                        "<div class='command'><span class='field-label'>命令</span>"
                        f"<code>{html.escape(self._display_command(command))}</code></div>"
                    )
            if kind == "agentMessage":
                status_text, result_text, highlight = (
                    "",
                    self._agent_message_text(group["events"]),
                    "",
                )
                if not full and len(result_text) > 420:
                    result_text = result_text[:419].rstrip() + "…"
            else:
                status_text, result_text, highlight = self._activity_result(group)
            status_style = ""
            if any(
                word in status_text
                for word in ("失败", "错误", "未完成", "已中断", "拒绝", "未批准")
            ):
                status_style = " status-error"
            elif any(word in status_text for word in ("成功", "已完成", "批准")):
                status_style = " status-success"
            status_block = (
                f"<div class='status{status_style}'>{html.escape(status_text)}</div>"
                if status_text
                else ""
            )
            preview = (
                highlight
                if highlight and len(result_text) > _REPORT_RESULT_PREVIEW_LIMIT
                else self._result_excerpt(result_text)
            )
            label_block = (
                f"<span class='field-label'>{result_label}</span>"
                if result_label
                else ""
            )
            result_block = (
                f"<div class='result{' result-error' if highlight else ''}'>"
                f"{label_block}"
                f"<div class='result-text'>{html.escape(preview)}</div></div>"
                if preview
                else ""
            )
            detail_block = (
                "<details class='result-detail'><summary>查看完整结果</summary>"
                f"<pre>{html.escape(result_text)}</pre></details>"
                if result_text and len(result_text) > _REPORT_RESULT_PREVIEW_LIMIT
                else ""
            )
            cards.append(
                f"<article class='activity kind-{style_kind}'>"
                "<div class='activity-head'><span class='activity-title'>"
                f"{title}</span><small>{time_label}</small></div>"
                f"{command_block}{status_block}{result_block}{detail_block}</article>"
            )
        if not cards:
            cards.append("<div class='empty'>暂无可展示的活动</div>")

        request_block = (
            "<section><h2>用户指令</h2>"
            f"<div class='request-text'>{html.escape(state.request_text)}</div></section>"
            if state.request_text
            else ""
        )
        error_block = f"<p class='error'>{html.escape(error)}</p>" if error else ""
        outcome_style = (
            "outcome-interrupted"
            if status == "已中断"
            else "outcome-error"
            if status == "失败"
            else "outcome-success"
        )
        hidden_count = max(0, len(groups) - len(visible_groups))
        note = (
            f"概览只显示最近 24 项；另有 {hidden_count} 项收录在完整记录中。"
            if hidden_count and not full
            else "较长的结果可展开查看保留的内容。"
            if full
            else ""
        )
        if state.activity_truncated:
            note += " 较早活动或过长内容已截断。"
        return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><style>
* {{ box-sizing: border-box; }}
body {{ margin: 0; padding: 28px; background: #f4f6f8; color: #17212b; font: 15px/1.5 "Segoe UI", "Microsoft YaHei", sans-serif; }}
main {{ width: 100%; max-width: 1000px; margin: 0 auto; }}
header {{ padding: 22px 24px; background: #fff; border: 1px solid #d9e0e5; border-left: 5px solid #138a72; }}
header.outcome-interrupted {{ border-left-color: #b77a38; }}
header.outcome-error {{ border-left-color: #b9574b; }}
h1 {{ margin: 0 0 12px; font-size: 23px; }}
.meta {{ display: flex; flex-wrap: wrap; gap: 8px 22px; color: #53616c; }}
section {{ margin-top: 14px; padding: 18px 22px; background: #fff; border: 1px solid #d9e0e5; }}
h2 {{ margin: 0 0 12px; font-size: 17px; }}
.request-text {{ white-space: pre-wrap; overflow-wrap: anywhere; }}
.activity {{ --accent: #82909b; --soft: #f8fafb; margin-top: 10px; padding: 14px 16px; border: 1px solid #e0e7eb; border-left: 4px solid var(--accent); border-radius: 8px; background: var(--soft); overflow-wrap: anywhere; }}
.kind-reply {{ --accent: #19896e; --soft: #f1faf7; }}
.kind-command {{ --accent: #5279b9; --soft: #f5f8fe; }}
.kind-tool {{ --accent: #8269ad; --soft: #f8f6fc; }}
.kind-file {{ --accent: #b77a38; --soft: #fff9f2; }}
.kind-reasoning {{ --accent: #82909b; --soft: #f7f9fa; }}
.kind-plan {{ --accent: #4d91a4; --soft: #f2f9fb; }}
.kind-approval {{ --accent: #b89a43; --soft: #fffbf0; }}
.activity-head {{ display: flex; justify-content: space-between; align-items: baseline; gap: 16px; }}
.activity-title {{ font-weight: 650; color: #1e2f39; }}
small {{ flex: 0 0 auto; color: #697681; }}
.field-label {{ display: block; margin-bottom: 5px; color: #697681; font-size: 12px; font-weight: 600; }}
.command {{ margin-top: 10px; padding: 10px 12px; border: 1px solid #dce6f5; border-radius: 5px; background: #fff; }}
.command code {{ display: block; white-space: pre-wrap; overflow-wrap: anywhere; font: 12px/1.5 Consolas, monospace; }}
.status {{ display: inline-block; margin-top: 10px; padding: 3px 9px; border-radius: 999px; background: #e9eef1; color: #52616b; font-size: 12px; }}
.status-success {{ background: #e1f3e9; color: #226b48; }}
.status-error {{ background: #fce9e6; color: #a34639; }}
.result {{ margin-top: 10px; padding: 10px 12px; border-radius: 5px; background: #fff; }}
.result-error {{ border: 1px solid #f1cfca; background: #fff7f5; }}
.result-text {{ color: #344652; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 13px; line-height: 1.6; }}
.kind-reply .result {{ background: transparent; padding: 4px 0 0; }}
.kind-reply .result-text {{ color: #1d4338; font-size: 14px; line-height: 1.75; }}
.result-detail {{ margin-top: 8px; }}
.result-detail summary {{ cursor: pointer; color: #315e80; font-size: 13px; }}
pre {{ max-height: 640px; overflow: auto; margin: 10px 0 4px; padding: 12px; background: #fff; border: 1px solid #e2e7ea; white-space: pre-wrap; overflow-wrap: anywhere; font: 12px/1.45 Consolas, monospace; }}
.empty,.note,.muted {{ color: #697681; }}
.note {{ margin: 12px 0 0; font-size: 12px; }}
.error {{ color: #a72e2e; }}
@media (max-width: 640px) {{ body {{ padding: 12px; }} header,section {{ padding: 16px; }} .activity-head {{ display: block; }} .activity-head small {{ display: block; margin-top: 2px; }} }}
</style></head><body><main>
<header class="{outcome_style}"><h1>Codex 执行记录</h1><div class="meta">
<span>本轮结果：{html.escape(status)}</span><span>耗时：{self._elapsed_text(state.started_at)}</span>
<span>模型：{html.escape(self._model_description(state.binding))}</span>
<span>活动：{len(groups)} 项</span>
</div>{error_block}</header>
{request_block}<section><h2>活动记录</h2>{''.join(cards)}<p class="note">{html.escape(note)}</p></section>
</main></body></html>"""

    async def _send_final_activity_report(
        self,
        state: ActiveTurn,
        *,
        status: str,
        error: str | None = None,
    ) -> None:
        if state.suppress_reply:
            return
        try:
            report_dir = self.data_dir / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_file = report_dir / (
                f"codex-{state.thread_id[:8]}-{uuid4().hex[:8]}.html"
            )
            report_html = self._activity_report_html(
                state, status=status, full=True, error=error
            )
            await asyncio.to_thread(
                report_file.write_text, report_html, encoding="utf-8"
            )
            if state.suppress_reply:
                return
            await state.event.send(
                MessageChain(
                    chain=[
                        Plain("[codex] 执行记录（HTML，可查看工具结果）"),
                        File(name=report_file.name, file=str(report_file)),
                    ]
                )
            )
        except Exception as exc:
            self.logger.exception("无法发送 Codex HTML 执行记录：%s", exc)
            await self._send_codex_reply(
                state.event, "执行记录生成或发送失败，请查看 AstrBot 日志。"
            )

    async def _interrupt(self, binding: CodexBinding, state: ActiveTurn) -> bool:
        if state.interrupt_requested:
            return True
        if state.completed.done():
            return False
        if state.turn_id is None:
            try:
                await asyncio.wait_for(state.turn_started.wait(), timeout=5)
            except asyncio.TimeoutError:
                return False
        if state.completed.done():
            return False
        if not state.turn_id:
            return False
        client = await self._ensure_client()
        state.interrupt_requested = True
        try:
            await client.request(
                "turn/interrupt",
                {"threadId": binding.thread_id, "turnId": state.turn_id},
            )
        except CodexAppServerError as exc:
            self.logger.debug("Codex interrupt request did not complete: %s", exc)
            return False
        return True

    async def _close_binding(self, event: AstrMessageEvent) -> str:
        binding = await self._require_binding(event)
        async with binding.lock:
            state = self.active_turns.get(binding.thread_id)
            if state and not state.completed.done():
                state.suppress_reply = True
                await self._interrupt(binding, state)
            await self._decline_approvals(binding.thread_id)
            self.bindings.pop(binding.umo, None)
            self._save_bindings()
        return "Codex 会话已关闭并解除绑定。Codex 中保存的线程历史仍然保留。"

    async def _decline_approvals(self, thread_id: str) -> None:
        matches = [
            approval
            for approval in self.pending_approvals.values()
            if self.bindings.get(approval.umo)
            and self.bindings[approval.umo].thread_id == thread_id
        ]
        for approval in matches:
            await self._resolve_approval(approval, approve=False)

    async def _answer_approval(self, event: AstrMessageEvent, *, approve: bool) -> str:
        umo = self._session_key(event)
        pending = sorted(
            (item for item in self.pending_approvals.values() if item.umo == umo),
            key=lambda item: item.details.get("startedAtMs", 0),
        )
        if not pending:
            return "当前没有等待处理的 Codex 审批。"
        await self._resolve_approval(pending[0], approve=approve)
        return "已批准 Codex 请求。" if approve else "已拒绝 Codex 请求。"

    async def _resolve_approval(
        self, approval: PendingApproval, *, approve: bool, expired: bool = False
    ) -> None:
        if self.pending_approvals.pop(approval.request_id, None) is None:
            return
        binding = self.bindings.get(approval.umo)
        state = self.active_turns.get(binding.thread_id) if binding else None
        if state:
            decision = "超时拒绝" if expired else ("已批准" if approve else "已拒绝")
            activity_params = dict(approval.details)
            activity_params.update(
                {
                    "requestId": approval.request_id,
                    "method": approval.method,
                    "decision": decision,
                }
            )
            self._record_activity(state, "serverRequest/resolved", activity_params)
            state.active_items.pop(f"approval:{approval.request_id}", None)
            state.current_activity = f"审批{decision}"
        current_task = asyncio.current_task()
        if approval.timer and approval.timer is not current_task:
            approval.timer.cancel()
        if not self.client:
            return
        try:
            await self.client.respond_to_server_request(
                approval.request_id,
                approval.method,
                {"decision": "accept" if approve else "decline"},
            )
            if expired:
                await self._send_text(
                    approval.event, "Codex 审批等待超时，本次请求已拒绝。"
                )
        except Exception as exc:
            self.logger.warning("Failed to answer Codex approval: %s", exc)

    async def _expire_approval(self, request_id: int | str) -> None:
        try:
            await asyncio.sleep(self.approval_timeout)
            approval = self.pending_approvals.get(request_id)
            if approval:
                await self._resolve_approval(approval, approve=False, expired=True)
        except asyncio.CancelledError:
            raise

    async def _model_catalog(self) -> list[dict[str, Any]]:
        client = await self._ensure_client()
        response = await client.request("model/list", {"limit": 100})
        models = response.get("data", []) if isinstance(response, dict) else []
        return [model for model in models if isinstance(model, dict)]

    async def _find_model(self, model_id: str) -> dict[str, Any]:
        models = await self._model_catalog()
        for model in models:
            if model_id in {model.get("id"), model.get("model")}:
                return model
        raise CodexAppServerError(
            f"未知模型 {model_id}。请先使用 /codex model 查看可用模型。"
        )

    @staticmethod
    def _model_efforts(model: dict[str, Any]) -> list[str]:
        options = model.get("supportedReasoningEfforts", [])
        if not isinstance(options, list):
            return []
        return [
            effort
            for option in options
            if isinstance(option, dict)
            and isinstance((effort := option.get("reasoningEffort")), str)
        ]

    async def _list_models(self) -> str:
        models = await self._model_catalog()
        if not models:
            return "Codex 没有返回可用模型。"
        lines = []
        for model in models[:40]:
            model_id = model.get("model", model.get("id", "未知"))
            display_name = model.get("displayName", "")
            efforts = self._model_efforts(model)
            default_effort = model.get("defaultReasoningEffort")
            line = f"{model_id} ({display_name})"
            if efforts:
                line += f"\n  思考等级：{', '.join(efforts)}"
                if isinstance(default_effort, str):
                    line += f"（默认：{default_effort}）"
            lines.append(line)
        suffix = "\n更多模型请通过 Codex 配置查看。" if len(models) >= 100 else ""
        return "可用模型和思考等级：\n" + "\n".join(lines) + suffix

    @staticmethod
    def _review_target(argument: str) -> dict[str, Any]:
        if not argument or argument == "uncommitted":
            return {"type": "uncommittedChanges"}
        kind, _, value = argument.partition(" ")
        if kind == "base" and value.strip():
            return {"type": "baseBranch", "branch": value.strip()}
        if kind == "commit" and value.strip():
            return {"type": "commit", "sha": value.strip(), "title": None}
        raise CodexAppServerError(
            "review 用法：/codex review [uncommitted|base <分支>|commit <SHA>]。"
        )

    @staticmethod
    def _help_text() -> str:
        return (
            "/codex：连接 Codex，继续使用原线程\n"
            "/codex help：显示此帮助\n"
            "/codex exit：断开当前聊天的 Codex 连接并保留线程\n"
            "/codex close：关闭绑定并中断任务\n"
            "/codex stop：中断当前任务\n"
            "/codex status、/codex pwd：查看状态和工作目录\n"
            "/codex new：新建线程\n"
            "/codex compact：压缩上下文\n"
            "/codex review [uncommitted|base <分支>|commit <SHA>]：代码审查\n"
            "/codex model [模型ID [思考等级]]：列出或切换模型\n"
            "/codex effort [思考等级]：查看或切换当前模型的思考等级\n"
            "/codex yes、/codex no：处理审批\n"
            "普通斜杠文本前加 // 可作为 Codex 任务发送。"
        )

    async def _on_notification(self, method: str, params: dict[str, Any]) -> None:
        thread_id = params.get("threadId")
        if not isinstance(thread_id, str):
            return
        state = self.active_turns.get(thread_id)
        if state is None:
            return
        notification_turn_id = self._notification_turn_id(method, params)
        if (
            notification_turn_id
            and state.turn_id
            and notification_turn_id != state.turn_id
        ):
            return
        if notification_turn_id and state.turn_id is None:
            state.turn_id = notification_turn_id
            state.turn_started.set()
        self._record_activity(state, method, params)
        self._activity_for_item(state, method, params)
        if method == "turn/started":
            turn = params.get("turn", {})
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if isinstance(turn_id, str):
                state.turn_id = turn_id
                state.turn_started.set()
                if state.suppress_reply:
                    self._track_task(self._interrupt(state.binding, state))
        elif method == "item/agentMessage/delta":
            turn_id = params.get("turnId")
            if state.turn_id is None or turn_id == state.turn_id:
                delta = params.get("delta")
                if isinstance(delta, str):
                    item_id = params.get("itemId")
                    if isinstance(item_id, str):
                        state.message_deltas.setdefault(item_id, []).append(delta)
                    else:
                        state.unassigned_deltas.append(delta)
        elif method == "item/completed":
            item = params.get("item")
            if isinstance(item, dict) and item.get("type") == "agentMessage":
                item_id = item.get("id")
                if isinstance(item_id, str) and item_id in state.sent_message_ids:
                    return
                completed_text = item.get("text")
                if not isinstance(completed_text, str) or not completed_text.strip():
                    completed_text = (
                        "".join(state.message_deltas.get(item_id, []))
                        if isinstance(item_id, str)
                        else "".join(state.unassigned_deltas)
                    )
                if completed_text.strip():
                    if isinstance(item_id, str):
                        state.sent_message_ids.add(item_id)
                        state.message_deltas.pop(item_id, None)
                    else:
                        state.unassigned_deltas.clear()
                    self._queue_codex_reply(state, completed_text)
        elif method == "turn/completed":
            turn = params.get("turn", {})
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if state.turn_id is None or turn_id == state.turn_id:
                if isinstance(turn_id, str):
                    state.turn_id = turn_id
                    state.turn_started.set()
                if not state.completed.done():
                    state.completed.set_result(turn if isinstance(turn, dict) else {})

    async def _on_server_request(self, request: dict[str, Any]) -> None:
        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})
        if not isinstance(request_id, (int, str)) or not isinstance(method, str):
            return
        if not isinstance(params, dict):
            params = {}
        thread_id = params.get("threadId")
        binding = next(
            (item for item in self.bindings.values() if item.thread_id == thread_id),
            None,
        )
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            if not binding or not self.client:
                if self.client:
                    await self.client.respond_to_server_request(
                        request_id, method, {"decision": "decline"}
                    )
                return
            state = self.active_turns.get(binding.thread_id)
            event = state.event if state else None
            if event is None:
                await self.client.respond_to_server_request(
                    request_id, method, {"decision": "decline"}
                )
                return
            approval = PendingApproval(
                request_id=request_id,
                method=method,
                umo=binding.umo,
                event=event,
                details=params,
            )
            self.pending_approvals[request_id] = approval
            approval.timer = asyncio.create_task(self._expire_approval(request_id))
            if state:
                activity_params = dict(params)
                activity_params["requestId"] = request_id
                self._record_activity(state, f"serverRequest/{method}", activity_params)
                state.active_items[f"approval:{request_id}"] = (
                    "等待命令执行审批"
                    if method == "item/commandExecution/requestApproval"
                    else "等待文件修改审批"
                )
                state.current_activity = state.active_items[f"approval:{request_id}"]
            if method == "item/commandExecution/requestApproval":
                command = str(params.get("command") or "(命令详情不可用)")[:1200]
                detail = f"Codex 请求执行命令：\n{command}"
            else:
                reason = str(params.get("reason") or "Codex 请求文件修改权限")[:1200]
                detail = f"Codex 请求文件修改审批：\n{reason}"
            await self._send_text(
                event, f"{detail}\n回复 /codex yes 批准，/codex no 拒绝。"
            )
            return

        if method == "item/permissions/requestApproval" and self.client:
            await self.client.respond_to_server_request(
                request_id,
                method,
                {"permissions": {}, "scope": "turn"},
            )
            if binding:
                state = self.active_turns.get(binding.thread_id)
                if state:
                    activity_params = dict(params)
                    activity_params.update(
                        {"requestId": request_id, "decision": "decline"}
                    )
                    self._record_activity(
                        state, f"serverRequest/{method}", activity_params
                    )
                    state.current_activity = "额外权限请求已拒绝"
                    await self._send_text(
                        state.event,
                        "Codex 请求额外权限；当前插件不提供粒度授权，本次未授予额外权限。",
                    )
            return

        if binding:
            state = self.active_turns.get(binding.thread_id)
            if state:
                activity_params = dict(params)
                activity_params["requestId"] = request_id
                self._record_activity(state, f"serverRequest/{method}", activity_params)
                state.current_activity = self._activity_title(
                    f"serverRequest/{method}", activity_params
                )
        if self.client:
            await self.client.reject_server_request(
                request_id, f"AstrBot Codex 插件不支持客户端请求：{method}"
            )

    async def _on_client_closed(self, error: Exception) -> None:
        self.loaded_threads.clear()
        for state in self.active_turns.values():
            if not state.completed.done():
                state.completed.set_result(
                    {"status": "failed", "error": {"message": str(error)}}
                )
        if (
            self.client
            and self.client.process
            and self.client.process.returncode is not None
        ):
            self.client = None

    async def _send_text(self, event: AstrMessageEvent, text: str) -> None:
        chunks = [text[index : index + 3500] for index in range(0, len(text), 3500)]
        for chunk in chunks or [""]:
            try:
                await event.send(MessageChain().message(chunk))
            except Exception as exc:
                self.logger.warning("Unable to deliver Codex response: %s", exc)
                return

    async def _send_codex_reply(self, event: AstrMessageEvent, text: str) -> None:
        await self._send_text(event, f"[codex] {text}")
