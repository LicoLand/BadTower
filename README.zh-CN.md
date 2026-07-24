# BadTower

**[English](README.md)** — 本 README 对的规范语言为英文（以 README.md 为准）；
本文件为本地化语言版本。

BadTower 是被有意视为不可信的人机协同通信节点。它只在 LicoUp 客户端之间
保存和转发不透明的加密信封，并提供邮箱、租约、配额、确认、过期和清理。

BadTower 无法解密内容，也不是身份、策略、权限、加密或客户端运行时权威。
客户端必须自行验证对端，并以端到端方式保护消息机密性与完整性。

本实现固定 Fabrigent `v1` 产物的 SHA-256；产物内容或治理边界变化时会关闭
服务。运行 `npm run verify` 验证。

## 文档

- [产品概述](PRODUCT.md)
- [文档索引](docs/README.md)
- [运维手册](docs/RUNBOOK.md)
- [兼容性](docs/COMPATIBILITY.md)
- [安全策略](SECURITY.md)
- [贡献指南](CONTRIBUTING.md)
- [变更日志](CHANGELOG.md)

许可证：[AGPL-3.0-or-later](LICENSE)。
