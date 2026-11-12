# 微信通道体验增强实施记录

目标：在现有 fork 的原生微信通道实现输入状态、分段回复、可选思考及工具提示；Miko 只管理连接。

基线：fork main cbdcebabf48c5ee2d6868b370326aa8341703f62。独立工作区，不复制原目录的未提交修改，不部署、不发送真实微信消息。

- [x] 配置及 PostgreSQL / SQLite 迁移：新增 show_thinking / show_tool_hints，默认 false；新通道 stream，旧通道保留模式。
- [x] 输入状态：票据、5 秒续期、3 秒请求超时、开始发送前取消及竞态保护。
- [x] 追加发送：2 秒最短间隔、2000 Unicode 字符、草稿过滤、完整正文持久化、失败不重跑模型。
- [x] WeKnora 设置及 Miko 重扫兼容。
- [x] 专项测试、构建、设置页截图及最终审查。

基线专项：internal/im、internal/im/wechat、internal/handler 通过。新增配置和协议测试已观察到预期失败。

实现裁定：上游显式 Replace 可能在工具结束后的模型恢复中替换已流出的正文。丢弃未发送旧缓冲；若已有正文可见，明确发送修订提示后发送新版本。不能伪装成微信能够撤回旧消息。


完成验证：IM 全部适配器与数据库竞态测试通过；Agent 竞态测试通过；PostgreSQL 18 一次性本地实例升级/回退通过；设置页实际组件浏览器夹具验证通过。WeKnora 前端类型检查、构建、2696 项单元测试及 16 项语言键检查通过；前端有 1 项既有跳过。

全量 handler 测试中的 TestPutTenantParserConfigAdminPreservesRedactedSecrets 返回 400 而非预期 200，已用原始 fork 源码 overlay 单独复现。未改动该解析器功能；其余 handler 测试另行执行，并在命令中显式排除此已知失败。

Miko API 微信隔离专项 59 项通过，3 项真实 Agent/WeKnora 测试因未连接外部服务跳过；微信入口 Python 2 项通过。Miko 完整 pnpm run check 通过，包含 H5 / 小程序构建及相关页面、兼容检查。

原 WeKnora 工作区未提交修改保留；独立分支本地交付，不推送、不部署、不发送真实微信消息。截图来自实际组件和隔离 API 夹具，不代表手机验收。
