# AstrBot Codex app-server

通过 Codex CLI 的 app-server JSON-RPC，在 AstrBot 私聊里持续控制 Codex。每个 AstrBot 会话绑定独立的 Codex 线程，工作目录由 AstrBot 当前会话工作区解析。

## 使用

- `/codex`：连接 Codex；已有线程时继续使用原线程，并显示当前模型、思考等级、工作目录和线程 ID。连接后，普通文本会交给 Codex，回复以 `[codex]` 开头。
- `/codex help`：显示所有可用命令。
- `/codex exit`：断开当前聊天与 Codex 的连接，保留线程和工作目录；再次发送 `/codex` 重新连接。
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

插件配置 `allow_group_chat` 默认关闭。开启后，AstrBot 管理员可在群聊中使用 `/codex` 连接；同一群的不同管理员共用一个 Codex 线程和工作目录，也都能发送任务及使用 Codex 命令。普通群成员的消息不会发送给 Codex。私聊仍仅限 AstrBot 管理员。

关闭或重启插件时，正在运行的 Codex 任务会停止，app-server 连接会关闭。插件再次启动后只保留原线程绑定，连接状态为“未连接”；普通消息不会自动发送给 Codex，需要发送 `/codex` 重新连接。

## 执行情况报告

Codex 每完成一条回复，插件就直接发送文本，不等待整轮任务结束。插件不会定时发送图片；需要查看运行时间和当前活动时，可随时使用 `/codex status`。审批请求仍会立即通知。

任务结束时的 HTML 执行报告由 AstrBot 插件配置 `send_final_report` 控制，默认关闭。关闭时不渲染 HTML，也不发送报告文件；Codex 回复仍会直接发送。在插件配置中开启后，使用工具、修改文件、更新计划、请求审批或失败的任务会在结束时附上报告；纯文本对话只发送 Codex 回复。

开启报告后，HTML 会呈现工具调用、命令输出、推理摘要、执行计划、文件变更和审批通知。状态变更、token 用量、任务开始/结束通知，以及没有正文的推理摘要不会出现在活动列表。连续的文本和输出增量会合并为一项活动。报告只展示活动名称、状态和实际结果，不展示原始事件 JSON；较长的命令输出和工具结果会显示首尾节选，并允许展开查看完整纯文本。推理摘要由模型和 Codex 配置决定；需要显示摘要时可将级别设为 `concise` 或 `detailed`，然后新建线程。执行记录不依赖 `astrbot_plugin_browser`。

Codex 线程的审批策略和沙箱模式由 Codex 自身配置决定，插件不会覆盖 `~/.codex/config.toml` 中的 `approval_policy` 和 `sandbox_mode`。若 Codex 按其配置发出审批请求，插件会在聊天中询问；额外权限请求不会被自动授予。TUI 专属斜杠命令不会逐项模拟，未映射命令会提示当前支持范围。

需要已安装并完成登录的 Codex CLI。插件通过标准输入/输出运行 `codex app-server --listen stdio://`，并在 AstrBot 重启后保留保存的线程绑定。
