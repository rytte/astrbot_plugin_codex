# AstrBot Codex app-server

通过 Codex CLI 的 app-server JSON-RPC，在 AstrBot 私聊里持续控制 Codex。每个 AstrBot 会话绑定独立的 Codex 线程，工作目录由 AstrBot 当前会话工作区解析。

## 使用

- `/codex`：创建或恢复连续对话，并显示当前模型、思考等级、工作目录和线程 ID。之后的普通文本会交给 Codex，回复以 `[codex]` 开头。
- `/codex help`：显示所有可用命令。
- `/codex exit`：切出连续对话，保留线程和工作目录；再次发送 `/codex` 恢复。
- `/codex close`：解除会话绑定并中断运行中的任务，不删除 Codex 保存的线程历史。
- `/codex stop`：中断当前任务。
- `/codex status`、`/codex pwd`：查看会话状态和工作目录；任务运行时，`status` 还会显示本轮已工作的时间。
- `/codex new`：在当前工作目录创建新线程。
- `/codex compact`：请求 app-server 压缩线程上下文。
- `/codex review`：审查未提交改动；也支持 `review base <分支>` 和 `review commit <SHA>`。
- `/codex model`：列出模型及各自支持的思考等级；`/codex model <模型ID> [思考等级]` 切换模型，可同时指定思考等级。
- `/codex effort`：查看当前模型支持的思考等级；`/codex effort <等级>` 单独切换当前线程的思考等级。
- `/codex summary <none|auto|concise|detailed>`：切换当前线程的推理摘要级别；有正文的摘要会保留在 HTML 执行记录中。
- `/codex yes`、`/codex no`：批准或拒绝 Codex 的命令执行和文件修改请求。
- `//` 开头的普通消息会去掉一个 `/` 后发送给 Codex，可用于发送斜杠开头的文本。

## 执行情况报告

Codex 每完成一条回复，插件就直接发送文本，不等待整轮任务结束。插件不会定时发送图片；需要查看运行时间和当前活动时，可随时使用 `/codex status`。审批请求仍会立即通知。

插件会收集工具调用、命令输出、推理摘要、执行计划、文件变更和审批通知。状态变更、token 用量、任务开始/结束通知，以及没有正文的推理摘要不会出现在活动列表。连续的文本和输出增量会合并为一项活动。使用工具、修改文件、更新计划、请求审批或失败的任务，结束后会附一份 HTML 执行记录；纯文本对话只发送 Codex 回复。报告中的工具等活动默认折叠，点击标题可查看参数和输出；Codex 回复直接显示正文。推理摘要由模型和 Codex 配置决定；需要显示摘要时可将级别设为 `concise` 或 `detailed`，然后新建线程。执行记录不依赖 `astrbot_plugin_browser`。

插件默认只接受管理员私聊。Codex 线程使用 `workspace-write` 沙箱和 `on-request` 审批策略。额外权限请求不会被自动授予；TUI 专属斜杠命令不会逐项模拟，未映射命令会提示当前支持范围。

需要已安装并完成登录的 Codex CLI。插件通过标准输入/输出运行 `codex app-server --listen stdio://`，并在 AstrBot 重启后恢复保存的线程绑定。
