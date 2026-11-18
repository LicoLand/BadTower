# BadTower

Updated: 2026-09-25

**[English](README.md)** — 本 README 对以英文版为规范文本；本文件为同步简体
中文本地化版本。

BadTower 是 LicoLand 的**服务层（Service Layer）**：一个纯粹、协议中立的
通讯站。原生通讯能力就是它的产品。

获批 Native Station Contract 直接提供对不透明 Transport Unit 的有界
Reserve、Submit、Claim、Settle，以及健康、容量、持久性、压力控制、恢复和
优雅关闭。它不包含 agent、task、tool、resource、session、federation、
endpoint trust 或外部协议语义，并且必须在没有插件时完整工作。

交付顺序不可颠倒：

1. 完成 Station Core；
2. 完成并验证一个单点 Station Instance；
3. 之后才把同一原生契约扩展成 Station Cluster；
4. 单点和集群原生卓越门禁通过后，才提供外置扩展契约；
5. 最后才实现被明确选择的外置 Protocol Compatibility Plugin。

LicoArc、A2A、ACP、MCP 都是可选外置插件计划，不是 Core 功能。每个插件都是
独立构建、打包、配置、监督、版本化、发布、支持和运营的进程，自有 listener、
协议代码、可选有界状态、资源预算、指标与测试。Core 从不发现、加载、嵌入或
监督插件。

一个插件的增删、崩溃、过载、升级、回滚或退役不得改变 Core 产物、原生契约、
持久化 schema 或其他插件。“插件需要”不足以扩大原生契约；只有对直接原生
client 独立有用且通过 Core 验收的能力才可进入 Core。

BadTower 单独拥有以后明确准入的任何 LicoArc 适配。LicoArc 独立开发和验证
协议层，不欠 BadTower adapter、fixture、test、package、release、support、
runner 或 receipt。

BadTower 不是 endpoint trust layer 或外部协议权威。Station Signal 不能证明
identity、authenticity、integrity、freshness、replay rejection、endpoint
receipt、approval、task completion、tool effect 或业务确认。观测不得暴露
payload、协议内容、key、credential、endpoint identifier、machine identity
或 raw runtime state。

当前源码实现一个原生 Station Instance 候选与部分 Cluster 共识基础：统一确定性
mutation machine、64 分区放置、ownership fencing、有界 in-flight etcd/raft、持久化重启
重放、quorum failover 和认证 peer。Cluster 动态生命周期、完整容量/恢复证据与
Native Excellence Gate 仍未关闭。

仓库也包含隔离的 External Extension Contract 与一个独立构建的精确 LicoArc
Candidate adapter。两者都是隔离候选源码：不进入 Core 依赖图；Native
Excellence 尚未通过时不作生产准入。验证、发布、支持与运营相互独立，不能互相
推导。详见[当前状态](docs/STATUS.md)。

仓库还实现了使用严格 manifest、分离 Ed25519 签名和有界独立校验的确定性
标准节点包。本地实现和校验不构成发布声明。

启动服务：

```sh
badtower --config /path/to/badtower.station.v1.json
```

校验配置：

```sh
badtowerctl check-config --config /path/to/badtower.station.v1.json
```

执行仓库验证：

```sh
go test -race ./...
go vet ./...
go build ./...
```

## 文档

- [产品概述（英文规范）](PRODUCT.md)
- [领域语言（英文规范）](CONTEXT.md)
- [当前状态（英文规范）](docs/STATUS.md)
- [文档索引（英文规范）](docs/README.md)
- [架构（英文规范）](docs/architecture/ARCHITECTURE.md)
- [运维手册](docs/RUNBOOK.md)
- [兼容性](docs/COMPATIBILITY.md)
- [标准节点包](docs/functionality/NODE-PACKAGE.md)
- [Station Instance 验收](docs/functionality/STATION-INSTANCE-ACCEPTANCE.md)
- [Station Cluster 基础](docs/functionality/STATION-CLUSTER.md)
- [Native Excellence Gate](docs/functionality/NATIVE-EXCELLENCE.md)
- [External Extension Contract](docs/functionality/EXTERNAL-EXTENSION-CONTRACT.md)
- [精确 LicoArc Candidate adapter](docs/protocols/LICOARC-CANDIDATE-ADAPTER.md)
- [安全策略](SECURITY.md)
- [贡献指南](CONTRIBUTING.md)
- [变更日志](CHANGELOG.md)

许可证：[AGPL-3.0-or-later](LICENSE)。
