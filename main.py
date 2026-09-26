"""Control Codex app-server threads from private AstrBot conversations."""

from __future__ import annotations

import asyncio
import html
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import File, Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.star.filter.command import GreedyStr
from astrbot.core.workspace import resolve_workspace_root_for_umo

from .app_server import AppServerClient, CodexAppServerError


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
    activity_events: list[dict[str, Any]] = field(default_factory=list)
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
        unknown = set(settings) - {"codex_command", "approval_timeout"}
        if unknown:
            raise ValueError("未知插件配置：" + ", ".join(sorted(unknown)))
        self.codex_command = str(settings.get("codex_command", "codex")).strip()
        self.approval_timeout = settings.get("approval_timeout", 180)
        if (
            type(self.approval_timeout) is not int
            or not 30 <= self.approval_timeout <= 900
        ):
            raise ValueError("approval_timeout 必须是 30～900 之间的整数。")

        self.data_dir = StarTools.get_data_dir("astrbot_plugin_codex")
        self.bindings_file = self.data_dir / "bindings.json"
        self.bindings: dict[str, CodexBinding] = {}
        self.active_turns: dict[str, ActiveTurn] = {}
        self.pending_approvals: dict[int | str, PendingApproval] = {}
        self.client: AppServerClient | None = None
        self.loaded_threads: set[str] = set()
        self.background_tasks: set[asyncio.Task] = set()

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
                        enabled=bool(value.get("enabled", True)),
                    )
        except FileNotFoundError:
            pass
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            self.logger.warning("无法读取 Codex 会话绑定：%s", exc)

    async def terminate(self) -> None:
        """Stop pending work and shut down the Codex app-server process."""
        for approval in list(self.pending_approvals.values()):
            if approval.timer:
                approval.timer.cancel()
        self.pending_approvals.clear()
        for state in self.active_turns.values():
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

    async def _workspace(self, event: AstrMessageEvent) -> Path:
        path = (
            Path(await resolve_workspace_root_for_umo(event.unified_msg_origin))
            .expanduser()
            .resolve(strict=False)
        )
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise CodexAppServerError(f"会话工作目录不可用：{path}")
        return path

    def _authorized(self, event: AstrMessageEvent) -> str | None:
        if not event.is_private_chat():
            return "Codex 插件目前只接受管理员私聊。"
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
            {
                "cwd": str(cwd),
                "approvalPolicy": "on-request",
                "sandbox": "workspace-write",
            },
        )
        thread = response.get("thread", {}) if isinstance(response, dict) else {}
        thread_id = thread.get("id")
        if not isinstance(thread_id, str) or not thread_id:
            raise CodexAppServerError("Codex 未返回有效的线程 ID。")
        binding = CodexBinding(
            umo=event.unified_msg_origin,
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
        umo = event.unified_msg_origin
        cwd = await self._workspace(event)
        binding = self.bindings.get(umo)
        if binding is None:
            binding = await self._create_binding(event)
            return (
                f"Codex 连续对话已开启。\n模型：{self._model_description(binding)}\n"
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
                    f"Codex 连续对话已恢复，当前任务仍在运行。\n"
                    f"模型：{self._model_description(binding)}\n工作目录：{binding.cwd}"
                )
            await self._resume_binding(binding, cwd=str(cwd))
            binding.cwd = str(cwd)
            binding.enabled = True
            await self._refresh_model(binding)
            self._save_bindings()
        return (
            f"Codex 连续对话已恢复。\n模型：{self._model_description(binding)}\n"
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
        binding = self.bindings.get(event.unified_msg_origin)
        if binding is None:
            raise CodexAppServerError("请先发送 /codex 开启 Codex 连续对话。")
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
                    "approvalPolicy": "on-request",
                    "sandbox": "workspace-write",
                },
            )
        except CodexAppServerError as exc:
            if "no rollout found" not in str(exc).lower():
                raise
            response = await client.request(
                "thread/start",
                {
                    "cwd": cwd or binding.cwd,
                    "approvalPolicy": "on-request",
                    "sandbox": "workspace-write",
                },
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
                result = "已切出 Codex 对话，线程和工作目录已保留。发送 /codex 可恢复。"
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
                        result = (
                            "已向 Codex 发送中断请求。"
                            if interrupted
                            else "暂未取得运行中任务的 ID，请稍后重试 /codex stop。"
                        )
            elif action == "status":
                binding = self.bindings.get(event.unified_msg_origin)
                if binding is None:
                    raise CodexAppServerError("请先发送 /codex 开启 Codex 连续对话。")
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
                    len(self._activity_groups(state.activity_events)) if state else 0
                )
                work_time = (
                    self._elapsed_text(state.started_at)
                    if running and state
                    else "当前无运行中的任务"
                )
                result = (
                    f"模式：{'连续对话' if binding.enabled else '已切出'}\n"
                    f"任务：{'运行中' if running else '空闲'}\n"
                    f"本轮工作时间：{work_time}\n"
                    f"当前活动：{activity}\n"
                    f"活动记录：{activity_count} 项\n"
                    f"模型：{self._model_description(binding)}\n"
                    f"工作目录：{binding.cwd}\n线程：{binding.thread_id}"
                )
            elif action == "pwd":
                binding = self.bindings.get(event.unified_msg_origin)
                if binding is None:
                    raise CodexAppServerError("请先发送 /codex 开启 Codex 连续对话。")
                result = binding.cwd
            elif action == "new":
                previous = self.bindings.get(event.unified_msg_origin)
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
            elif action == "summary":
                binding = await self._require_binding(event)
                requested_summary = argument.lower().strip()
                choices = {"none", "auto", "concise", "detailed"}
                if requested_summary not in choices:
                    raise CodexAppServerError(
                        "summary 用法：/codex summary <none|auto|concise|detailed>。"
                    )
                async with binding.lock:
                    self._ensure_idle(binding)
                    client = await self._ensure_client()
                    await client.request(
                        "thread/settings/update",
                        {
                            "threadId": binding.thread_id,
                            "summary": requested_summary,
                        },
                    )
                result = f"当前线程的推理摘要级别已设为 {requested_summary}。"
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
                    "help、exit、close、yes、no、stop、status、pwd、new、compact、review、model、effort、summary。"
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
        binding = self.bindings.get(event.unified_msg_origin)
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
        state = ActiveTurn(
            binding=binding,
            thread_id=binding.thread_id,
            event=event,
            completed=asyncio.get_running_loop().create_future(),
            success_message=success_message,
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
            if self._should_send_final_report(state, status=report_status):
                await self._send_final_activity_report(state, status=report_status)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not state.suppress_reply:
                state.current_activity = "失败"
                await self._send_codex_reply(state.event, f"Codex 任务失败：{exc}")
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

    def _record_activity(
        self, state: ActiveTurn, method: str, params: dict[str, Any]
    ) -> None:
        if method in self._HIDDEN_ACTIVITY_METHODS:
            return
        raw_item = params.get("item")
        if isinstance(raw_item, dict) and raw_item.get("type") == "userMessage":
            return
        snapshot = json.loads(json.dumps(params, ensure_ascii=False, default=str))
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
                        previous_params["delta"] = (
                            str(previous_params.get("delta") or "") + snapshot["delta"]
                        )
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
    def _activity_groups(cls, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            visible.append(group)
        return visible

    @staticmethod
    def _should_send_final_report(state: ActiveTurn, *, status: str) -> bool:
        if status != "已完成":
            return True
        for event in state.activity_events:
            method = event.get("method", "")
            if method.startswith("serverRequest/"):
                return True
            if method.startswith(
                (
                    "item/commandExecution/",
                    "item/fileChange/",
                    "item/mcpToolCall/",
                    "item/plan/",
                )
            ):
                return True
            params = event.get("params")
            item = params.get("item") if isinstance(params, dict) else None
            if isinstance(item, dict) and item.get("type") not in {
                "userMessage",
                "agentMessage",
                "reasoning",
            }:
                return True
        return False

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
        current = state.current_activity
        if state.active_items:
            current = list(state.active_items.values())[-1]

        cards = []
        for group in visible_groups:
            title = html.escape(str(group["title"]))
            time_label = html.escape(
                str(group["first_time"])
                if group["first_time"] == group["last_time"]
                else f"{group['first_time']} - {group['last_time']}"
            )
            if group["kind"] == "agentMessage":
                reply = self._agent_message_text(group["events"])
                if not full and len(reply) > 420:
                    reply = reply[:419].rstrip() + "…"
                cards.append(
                    "<div class='activity'><div class='activity-body'><div><span>"
                    f"{title}</span><small>{time_label}</small></div>"
                    f"<div class='preview'>{html.escape(reply)}</div></div></div>"
                )
                continue
            serialized = json.dumps(group["events"], ensure_ascii=False, indent=2)
            preview = html.escape(self._activity_preview(group["events"]))
            preview_block = f"<div class='preview'>{preview}</div>" if preview else ""
            if full:
                cards.append(
                    "<details class='activity'><summary>"
                    f"<span>{title}</span><small>{time_label}</small>"
                    f"</summary>{preview_block}<pre>"
                    f"{html.escape(serialized)}"
                    "</pre></details>"
                )
            else:
                cards.append(
                    "<div class='activity'><div class='activity-body'><div><span>"
                    f"{title}</span><small>{time_label}</small></div>"
                    f"{preview_block}</div></div>"
                )
        if not cards:
            cards.append("<div class='empty'>暂无可展示的活动</div>")

        error_block = f"<p class='error'>{html.escape(error)}</p>" if error else ""
        hidden_count = max(0, len(groups) - len(visible_groups))
        note = (
            f"概览只显示最近 24 项；另有 {hidden_count} 项收录在完整记录中。"
            if hidden_count and not full
            else "工具等活动可展开查看参数与输出。"
            if full
            else ""
        )
        return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><style>
* {{ box-sizing: border-box; }}
body {{ margin: 0; padding: 28px; background: #f4f6f8; color: #17212b; font: 15px/1.5 "Segoe UI", "Microsoft YaHei", sans-serif; }}
main {{ width: 100%; max-width: 1000px; margin: 0 auto; }}
header {{ padding: 22px 24px; background: #fff; border: 1px solid #d9e0e5; border-left: 5px solid #138a72; }}
h1 {{ margin: 0 0 12px; font-size: 23px; }}
.meta {{ display: flex; flex-wrap: wrap; gap: 8px 22px; color: #53616c; }}
.current {{ margin-top: 13px; padding-top: 12px; border-top: 1px solid #e5e9ec; }}
section {{ margin-top: 14px; padding: 18px 22px; background: #fff; border: 1px solid #d9e0e5; }}
h2 {{ margin: 0 0 12px; font-size: 17px; }}
.activity {{ display: flex; justify-content: space-between; gap: 16px; padding: 10px 0; border-top: 1px solid #edf0f2; overflow-wrap: anywhere; }}
.activity-body {{ width: 100%; min-width: 0; }}
.activity-body > div:first-child {{ display: flex; justify-content: space-between; gap: 16px; }}
details.activity {{ display: block; }}
summary {{ display: flex; justify-content: space-between; gap: 16px; cursor: pointer; }}
small {{ flex: 0 0 auto; color: #697681; }}
.preview {{ margin-top: 6px; color: #344652; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 13px; }}
pre {{ max-height: none; margin: 10px 0 4px; padding: 12px; background: #f4f6f8; border: 1px solid #e2e7ea; white-space: pre-wrap; overflow-wrap: anywhere; font: 12px/1.45 Consolas, monospace; }}
.empty,.note,.muted {{ color: #697681; }}
.note {{ margin: 12px 0 0; font-size: 12px; }}
.error {{ color: #a72e2e; }}
</style></head><body><main>
<header><h1>Codex 执行情况</h1><div class="meta">
<span>状态：{html.escape(status)}</span><span>耗时：{self._elapsed_text(state.started_at)}</span>
<span>模型：{html.escape(self._model_description(state.binding))}</span>
<span>活动：{len(groups)} 项</span>
</div><div class="current">当前活动：{html.escape(current)}</div>{error_block}</header>
<section><h2>活动记录</h2>{''.join(cards)}<p class="note">{html.escape(note)}</p></section>
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
                        Plain("[codex] 执行记录（HTML，可展开查看工具详情）"),
                        File(name=report_file.name, file=str(report_file)),
                    ]
                )
            )
        except Exception as exc:
            self.logger.warning("无法发送 Codex HTML 执行记录：%s", exc)

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
        umo = event.unified_msg_origin
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
            "/codex：开启或恢复连续对话\n"
            "/codex help：显示此帮助\n"
            "/codex exit：切出并保留线程\n"
            "/codex close：关闭绑定并中断任务\n"
            "/codex stop：中断当前任务\n"
            "/codex status、/codex pwd：查看状态和工作目录\n"
            "/codex new：新建线程\n"
            "/codex compact：压缩上下文\n"
            "/codex review [uncommitted|base <分支>|commit <SHA>]：代码审查\n"
            "/codex model [模型ID [思考等级]]：列出或切换模型\n"
            "/codex effort [思考等级]：查看或切换当前模型的思考等级\n"
            "/codex summary <none|auto|concise|detailed>：切换推理摘要级别\n"
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
