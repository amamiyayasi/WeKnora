# 原生微信通道体验增强：本地验证

代码基线：`cbdcebabf48c5ee2d6868b370326aa8341703f62`；分支：`codex/wechat-progress`。未部署、未推送、未向真实微信发送消息。

## 已验证

- `go test -race -tags sqlite_fts5 ./internal/im/... ./internal/database ./internal/agent -count=1`：通过。签名 URL 清理调整后再次执行全部 IM 适配器及数据库竞态回归，通过。
- 票据请求、业务错误、输入状态超时与续期、取消竞态、通道/用户隔离、发送节流、草稿过滤、正文去重、修订通知、错误停止排队及完整正文保存均有专项测试。
- PostgreSQL 18 一次性本地数据库执行新增迁移和回退，旧 full 模式保持不变；SQLite 新建/历史升级及回退测试通过。
- handler 全量检查存在下述原始失败；显式跳过该项后其余 handler 竞态测试通过。
- 前端 `npm run type-check`、`npm run build`、`npm test`、`npm run check-i18n` 通过：2696 项单元测试通过，1 项既有跳过；16 项语言键检查通过。构建仍有上游包体大小告警。
- 浏览器使用实际 IMChannelPanel、TDesign 和 Pinia，API 为隔离夹具：两个开关独立保存、full 模式禁用且保留值、已有绑定保存时不提交凭据、重新打开后值一致。

## 已知原始失败与限制

`TestPutTenantParserConfigAdminPreservesRedactedSecrets` 期望 HTTP 200，实际 400。使用原始 fork 的 Go 源码 overlay 单独运行也失败，不属于本次微信改动。其余 handler 运行命令显式包含 `-skip '^TestPutTenantParserConfigAdminPreservesRedactedSecrets$'`，不把该检查描述为无条件全量通过。

真实微信输入状态显示、真实分段送达、官方频控和手机交互未验证。普通 ReAct 草稿保留至轮次确认；只有工具结束后的明确最终合成允许提前输出。上游 Replace 发生在已发正文之后时，微信无法撤回旧气泡，必须标注修订后发送新版本。

部署前先核对运行版本与既有定制补丁，不用本地干净 fork 覆盖未盘点的现网定制。升级/回退顺序见 IM 功能文档。
