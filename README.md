# AstrBot Codex app-server

通过 Codex CLI 的 app-server JSON-RPC，在 AstrBot 私聊里持续控制 Codex。每个 AstrBot 会话绑定独立的 Codex 线程，工作目录由 AstrBot 当前会话工作区解析。

## 使用

- `/codex`：创建或恢复连续对话，并显示当前模型、思考等级、工作目录和线程 ID。之后的普通文本会交给 Codex，回复以 `[codex]` 开头。
- `/codex exit`：切出连续对话，保留线程和工作目录；再次发送 `/codex` 恢复。
- `/codex close`：解除会话绑定并中断运行中的任务，不删除 Codex 保存的线程历史。
- `/codex stop`：中断当前任务。
- `/codex status`、`/codex pwd`：查看会话状态和工作目录。
- `/codex new`：在当前工作目录创建新线程。
- `/codex compact`：请求 app-server 压缩线程上下文。
- `/codex review`：审查未提交改动；也支持 `review base <分支>` 和 `review commit <SHA>`。
- `/codex model`：列出模型及各自支持的思考等级；`/codex model <模型ID> [思考等级]` 切换模型，可同时指定思考等级。
- `/codex effort`：查看当前模型支持的思考等级；`/codex effort <等级>` 单独切换当前线程的思考等级。
- `/codex yes`、`/codex no`：批准或拒绝 Codex 的命令执行和文件修改请求。
- `//` 开头的普通消息会去掉一个 `/` 后发送给 Codex，可用于发送斜杠开头的文本。

插件默认只接受管理员私聊。Codex 线程使用 `workspace-write` 沙箱和 `on-request` 审批策略。额外权限请求不会被自动授予；TUI 专属斜杠命令不会逐项模拟，未映射命令会提示当前支持范围。

需要已安装并完成登录的 Codex CLI。插件通过标准输入/输出运行 `codex app-server --listen stdio://`，并在 AstrBot 重启后恢复保存的线程绑定。
