# BadTower

**[English](README.md)** — 本 README 对的规范语言为英文（以 README.md 为准）；
本文件为本地化语言版本。

BadTower 是 Lico Arc Network 中通用、独立运营的单点通讯站。任何端点都
默认把它视为恶意站点；它只保存和转发不透明的 Lico Arc Protocol 运输单元。

LicoUp 和其他端点可以让通信经过 BadTower，但绝不把它作为可信产品对接，
也不向它委托身份、加密、完整性、送达或安全权威。HTTP 响应、存储、队列、
租约、确认和回执都只是站点本地、不可置信的运输提示；端点安全始终由端到端
机制保证。

BadTower 不依赖 LicoArc 仓库的源码、构建、内置产物或运行时。默认本地中继
profile 接受已发布的 `licoarc.relay.v1` 线路标识，但运维配额和发布周期由
BadTower 自己维护。

BadTower 使用 Go 实现。HTTP 服务把不透明运输单元持久化到本地 bbolt 数据
库，并提供站点本地的租约、投递、拉取和确认操作。

启动服务：

```sh
go run ./cmd/badtower
```

执行仓库验证：

```sh
go test -race ./...
go vet ./...
go build ./...
```

## 文档

- [产品概述](PRODUCT.md)
- [文档索引](docs/README.md)
- [运维手册](docs/RUNBOOK.md)
- [兼容性](docs/COMPATIBILITY.md)
- [安全策略](SECURITY.md)
- [贡献指南](CONTRIBUTING.md)
- [变更日志](CHANGELOG.md)

许可证：[AGPL-3.0-or-later](LICENSE)。
