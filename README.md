# BadTower

**[简体中文](README.zh-CN.md)** — English is the normative language of this
README pair; the Simplified Chinese version is the localized language version.

BadTower is an intentionally untrusted communication node. It stores and
forwards opaque encrypted envelopes between LicoUp clients and implements only
mailboxes, leases, quotas, acknowledgements, expiry, and cleanup.

BadTower cannot decrypt content and is not an identity, policy, permission,
encryption, or client-runtime authority. Clients must authenticate peers and
protect message confidentiality and integrity end to end.

This implementation pins the vendored Fabrigent `v1` artifact by SHA-256 and
fails closed when its content or governance boundary changes.

Run `npm run verify`.

## Documentation

- [Product overview](PRODUCT.md)
- [Documentation index](docs/README.md)
- [Runbook](docs/RUNBOOK.md)
- [Compatibility](docs/COMPATIBILITY.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

License: [AGPL-3.0-or-later](LICENSE).
