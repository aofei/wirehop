# Changelog

## [0.2.0](https://github.com/aofei/wirehop/compare/v0.1.0...v0.2.0) (2026-09-09)


### Features

* add batched forwarding and resilient network recovery ([e1de96f](https://github.com/aofei/wirehop/commit/e1de96f3b308ae1a703688ff43882cc0b9e9d17d))
* add direct WireGuard UDP forwarding ([7ad5ad4](https://github.com/aofei/wirehop/commit/7ad5ad42c7f07d81b6531b5adc1ba96cde42eb14))


### Bug Fixes

* correct UDP recovery and streamline acknowledgment cleanup ([ca4dae2](https://github.com/aofei/wirehop/commit/ca4dae20b8d71988bc3c41d8c8b0d31fef79f134))
* limit the UDP GSO workaround to affected Linux releases ([c165c2c](https://github.com/aofei/wirehop/commit/c165c2c146bd21fafd5eec72e2c57a8711378e15))


### Tests

* wait for the confirmed session before reconnecting a lane ([bb56f02](https://github.com/aofei/wirehop/commit/bb56f02dd2abd4ca76e59e5d5400c23ee7e6ca88))


### Miscellaneous Chores

* bump Go to 1.27 ([fd0e6d8](https://github.com/aofei/wirehop/commit/fd0e6d88e2a90df7b42cccc10ec6ac4d59f2ac6a))
* release 0.2.0 ([c259fbe](https://github.com/aofei/wirehop/commit/c259fbe5dae63dffcc3014e64c6f66acb9540acd))

## 0.1.0 (2026-08-16)


### Features

* implement multipath WireGuard relay ([38424e1](https://github.com/aofei/wirehop/commit/38424e17f05870d210cb9caf7032dd10165930e0))


### Bug Fixes

* **laneurl:** stabilize unbracketed IPv6 diagnostics ([6ff6c41](https://github.com/aofei/wirehop/commit/6ff6c4188cb72c9867ccc4ad74dab47952c8f9dc))


### Tests

* **server:** stabilize raw rejection timing ([893a9b5](https://github.com/aofei/wirehop/commit/893a9b542a76ce662207bb4277227c9099e74cb8))


### Miscellaneous Chores

* add LICENSE ([efe43f7](https://github.com/aofei/wirehop/commit/efe43f77a4f9fb413a3a09bbd2d86fa52fa0ddeb))
* release 0.1.0 ([9e6e505](https://github.com/aofei/wirehop/commit/9e6e50503a5cb19d0536f575e4562ed3723ede9c))
* **release:** add automated release packaging ([cce04d3](https://github.com/aofei/wirehop/commit/cce04d3b8e5aaf0c95d5338e606d67469767c5ad))
